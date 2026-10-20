package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	_ "github.com/mattn/go-sqlite3"
)

var (
	db         *sql.DB
	ftsEnabled bool
	clients    = make(map[*websocket.Conn]bool)
	clientsMu  sync.RWMutex
	upgrader   = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     func(r *http.Request) bool { return true },
	}
)

type Block struct {
	ID               int64  `json:"id"`
	BlockType        string `json:"block_type"`
	RawContent       string `json:"raw_content"`
	Annotation       string `json:"annotation"`
	FileName         string `json:"file_name"`
	FilePath         string `json:"file_path"`
	MimeType         string `json:"mime_type"`
	FileSize         int64  `json:"file_size"`
	Space            string `json:"space"`
	ExtractedContent string `json:"extracted_content"`
	CreatedAt        string `json:"created_at"`
}

type WSEvent struct {
	Type  string `json:"type"`
	Block *Block `json:"block,omitempty"`
	ID    int64  `json:"id,omitempty"`
}

func initDB() {
	var err error
	os.MkdirAll("./data/uploads", 0755)
	db, err = sql.Open("sqlite3", "./data/streamspace.db?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	db.SetMaxOpenConns(1)

	// 1. Create base blocks table
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS blocks (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		block_type TEXT NOT NULL DEFAULT 'text',
		raw_content TEXT NOT NULL DEFAULT '',
		annotation TEXT NOT NULL DEFAULT '',
		file_name TEXT NOT NULL DEFAULT '',
		file_path TEXT NOT NULL DEFAULT '',
		mime_type TEXT NOT NULL DEFAULT '',
		file_size INTEGER NOT NULL DEFAULT 0,
		space TEXT NOT NULL DEFAULT 'Unfiled',
		extracted_content TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL DEFAULT ''
	)`)
	if err != nil {
		log.Fatal("Failed to create blocks table:", err)
	}

	// 2. Try initializing FTS5 virtual table
	_, err = db.Exec(`CREATE VIRTUAL TABLE IF NOT EXISTS blocks_fts USING fts5(
		raw_content, annotation, file_name, extracted_content, space,
		content='blocks',
		content_rowid='id'
	)`)

	if err == nil {
		ftsEnabled = true
		log.Println("FTS5 full-text search engine initialized")
		db.Exec(`CREATE TRIGGER IF NOT EXISTS blocks_ai AFTER INSERT ON blocks BEGIN
			INSERT INTO blocks_fts(rowid, raw_content, annotation, file_name, extracted_content, space)
			VALUES (new.id, new.raw_content, new.annotation, new.file_name, new.extracted_content, new.space);
		END`)
		db.Exec(`CREATE TRIGGER IF NOT EXISTS blocks_ad AFTER DELETE ON blocks BEGIN
			INSERT INTO blocks_fts(blocks_fts, rowid, raw_content, annotation, file_name, extracted_content, space)
			VALUES ('delete', old.id, old.raw_content, old.annotation, old.file_name, old.extracted_content, old.space);
		END`)
		db.Exec(`CREATE TRIGGER IF NOT EXISTS blocks_au AFTER UPDATE ON blocks BEGIN
			INSERT INTO blocks_fts(blocks_fts, rowid, raw_content, annotation, file_name, extracted_content, space)
			VALUES ('delete', old.id, old.raw_content, old.annotation, old.file_name, old.extracted_content, old.space);
			INSERT INTO blocks_fts(rowid, raw_content, annotation, file_name, extracted_content, space)
			VALUES (new.id, new.raw_content, new.annotation, new.file_name, new.extracted_content, new.space);
		END`)
	} else {
		ftsEnabled = false
		log.Printf("FTS5 module not active in SQLite build, using indexed LIKE search fallback: %v", err)
		db.Exec("DROP TRIGGER IF EXISTS blocks_ai")
		db.Exec("DROP TRIGGER IF EXISTS blocks_ad")
		db.Exec("DROP TRIGGER IF EXISTS blocks_au")
	}

	log.Println("Database initialized successfully")
}

func broadcast(event WSEvent) {
	data, err := json.Marshal(event)
	if err != nil {
		log.Println("broadcast marshal error:", err)
		return
	}
	clientsMu.RLock()
	defer clientsMu.RUnlock()
	for conn := range clients {
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			log.Println("broadcast write error:", err)
			conn.Close()
			go func(c *websocket.Conn) {
				clientsMu.RUnlock()
				clientsMu.Lock()
				delete(clients, c)
				clientsMu.Unlock()
				clientsMu.RLock()
			}(conn)
		}
	}
}

func handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("websocket upgrade error:", err)
		return
	}
	clientsMu.Lock()
	clients[conn] = true
	clientsMu.Unlock()
	log.Printf("WebSocket client connected (%d total)", len(clients))

	defer func() {
		clientsMu.Lock()
		delete(clients, conn)
		clientsMu.Unlock()
		conn.Close()
		log.Printf("WebSocket client disconnected (%d total)", len(clients))
	}()

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

func getBlockByID(id int64) (*Block, error) {
	b := &Block{}
	err := db.QueryRow(`SELECT id, block_type, raw_content, annotation, file_name, file_path,
		mime_type, file_size, space, extracted_content, created_at FROM blocks WHERE id = ?`, id).Scan(
		&b.ID, &b.BlockType, &b.RawContent, &b.Annotation, &b.FileName, &b.FilePath,
		&b.MimeType, &b.FileSize, &b.Space, &b.ExtractedContent, &b.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func handleGetBlocks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	space := r.URL.Query().Get("space")
	blockType := r.URL.Query().Get("type")

	var rows *sql.Rows
	var err error

	if q != "" {
		if ftsEnabled {
			cleanQ := strings.ReplaceAll(q, `"`, `""`)
			ftsQuery := `"` + cleanQ + `"*`
			query := `SELECT b.id, b.block_type, b.raw_content, b.annotation, b.file_name, b.file_path,
				b.mime_type, b.file_size, b.space, b.extracted_content, b.created_at
				FROM blocks b
				JOIN blocks_fts f ON b.id = f.rowid
				WHERE blocks_fts MATCH ?`
			args := []interface{}{ftsQuery}

			if space != "" && space != "All" {
				query += " AND b.space = ?"
				args = append(args, space)
			}
			if blockType != "" && blockType != "all" {
				query += " AND b.block_type = ?"
				args = append(args, blockType)
			}
			query += " ORDER BY b.created_at DESC LIMIT 200"
			rows, err = db.Query(query, args...)
		} else {
			like := "%" + q + "%"
			query := `SELECT id, block_type, raw_content, annotation, file_name, file_path,
				mime_type, file_size, space, extracted_content, created_at FROM blocks
				WHERE (raw_content LIKE ? OR annotation LIKE ? OR file_name LIKE ? OR extracted_content LIKE ? OR space LIKE ?)`
			args := []interface{}{like, like, like, like, like}

			if space != "" && space != "All" {
				query += " AND space = ?"
				args = append(args, space)
			}
			if blockType != "" && blockType != "all" {
				query += " AND block_type = ?"
				args = append(args, blockType)
			}
			query += " ORDER BY created_at DESC LIMIT 200"
			rows, err = db.Query(query, args...)
		}
	} else {
		query := `SELECT id, block_type, raw_content, annotation, file_name, file_path,
			mime_type, file_size, space, extracted_content, created_at FROM blocks WHERE 1=1`
		args := []interface{}{}

		if space != "" && space != "All" {
			query += " AND space = ?"
			args = append(args, space)
		}
		if blockType != "" && blockType != "all" {
			query += " AND block_type = ?"
			args = append(args, blockType)
		}
		query += " ORDER BY created_at DESC LIMIT 200"
		rows, err = db.Query(query, args...)
	}

	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	blocks := []Block{}
	for rows.Next() {
		var b Block
		if err := rows.Scan(&b.ID, &b.BlockType, &b.RawContent, &b.Annotation, &b.FileName,
			&b.FilePath, &b.MimeType, &b.FileSize, &b.Space, &b.ExtractedContent, &b.CreatedAt); err != nil {
			log.Println("scan error:", err)
			continue
		}
		blocks = append(blocks, b)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(blocks)
}

func determineBlockType(mimeType, rawContent, fileName string) string {
	mt := strings.ToLower(mimeType)
	if strings.HasPrefix(mt, "image/") {
		return "image"
	}
	if mt == "application/pdf" || strings.HasSuffix(strings.ToLower(fileName), ".pdf") {
		return "pdf"
	}
	if strings.Contains(rawContent, "http://") || strings.Contains(rawContent, "https://") {
		trimmed := strings.TrimSpace(rawContent)
		if strings.HasPrefix(trimmed, "http://") || strings.HasPrefix(trimmed, "https://") {
			return "link"
		}
	}
	codeExts := []string{".go", ".py", ".js", ".ts", ".java", ".c", ".cpp", ".rs", ".rb", ".sh", ".sql", ".yaml", ".yml", ".json", ".xml", ".html", ".css"}
	ext := strings.ToLower(filepath.Ext(fileName))
	for _, ce := range codeExts {
		if ext == ce {
			return "code"
		}
	}
	return "text"
}

func handleCreateBlock(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(50 << 20); err != nil {
		log.Printf("ParseMultipartForm error: %v", err)
		http.Error(w, `{"error":"parse form: `+err.Error()+`"}`, http.StatusBadRequest)
		return
	}

	rawContent := r.FormValue("raw_content")
	space := r.FormValue("space")
	annotation := r.FormValue("annotation")
	if space == "" {
		space = "Unfiled"
	}

	var fileName, filePath, mimeType string
	var fileSize int64

	file, header, err := r.FormFile("file")
	if err == nil {
		defer file.Close()
		fileName = header.Filename
		mimeType = header.Header.Get("Content-Type")
		if mimeType == "" {
			mimeType = mime.TypeByExtension(filepath.Ext(fileName))
			if mimeType == "" {
				mimeType = "application/octet-stream"
			}
		}

		hasher := sha256.New()
		tmpBuf, err := io.ReadAll(file)
		if err != nil {
			http.Error(w, `{"error":"read file: `+err.Error()+`"}`, http.StatusInternalServerError)
			return
		}
		hasher.Write(tmpBuf)
		hashStr := hex.EncodeToString(hasher.Sum(nil))
		ext := filepath.Ext(fileName)
		storedName := hashStr + ext
		filePath = filepath.Join("data", "uploads", storedName)

		if err := os.WriteFile(filePath, tmpBuf, 0644); err != nil {
			http.Error(w, `{"error":"save file: `+err.Error()+`"}`, http.StatusInternalServerError)
			return
		}
		fileSize = int64(len(tmpBuf))
		if rawContent == "" {
			rawContent = fileName
		}
	}

	blockType := determineBlockType(mimeType, rawContent, fileName)
	createdAt := time.Now().UTC().Format(time.RFC3339)

	res, err := db.Exec(`INSERT INTO blocks (block_type, raw_content, annotation, file_name, file_path, mime_type, file_size, space, extracted_content, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', ?)`,
		blockType, rawContent, annotation, fileName, filePath, mimeType, fileSize, space, createdAt)
	if err != nil {
		log.Printf("DB insert error: %v", err)
		http.Error(w, `{"error":"insert: `+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	id, _ := res.LastInsertId()
	block, err := getBlockByID(id)
	if err != nil {
		log.Printf("Get block by id error: %v", err)
		http.Error(w, `{"error":"fetch: `+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	go enrichBlock(block)

	broadcast(WSEvent{Type: "BLOCK_CREATED", Block: block})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(block)
}

func handleUpdateBlock(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		ID         int64  `json:"id"`
		Space      string `json:"space"`
		Annotation string `json:"annotation"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, `{"error":"decode: `+err.Error()+`"}`, http.StatusBadRequest)
		return
	}

	_, err := db.Exec(`UPDATE blocks SET space = ?, annotation = ? WHERE id = ?`,
		payload.Space, payload.Annotation, payload.ID)
	if err != nil {
		http.Error(w, `{"error":"update: `+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	block, err := getBlockByID(payload.ID)
	if err != nil {
		http.Error(w, `{"error":"fetch: `+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	broadcast(WSEvent{Type: "BLOCK_UPDATED", Block: block})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(block)
}

func handleDeleteBlock(w http.ResponseWriter, r *http.Request) {
	idStr := r.URL.Query().Get("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, `{"error":"invalid id"}`, http.StatusBadRequest)
		return
	}

	block, _ := getBlockByID(id)
	if block != nil && block.FilePath != "" {
		os.Remove(block.FilePath)
	}

	_, err = db.Exec(`DELETE FROM blocks WHERE id = ?`, id)
	if err != nil {
		http.Error(w, `{"error":"delete: `+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	broadcast(WSEvent{Type: "BLOCK_DELETED", ID: id})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"deleted": id})
}

func handleServeFile(w http.ResponseWriter, r *http.Request) {
	fp := r.URL.Query().Get("path")
	if fp == "" {
		http.Error(w, "missing path", http.StatusBadRequest)
		return
	}
	fp = strings.ReplaceAll(fp, "..", "")
	mt := mime.TypeByExtension(filepath.Ext(fp))
	if mt != "" {
		w.Header().Set("Content-Type", mt)
	}
	http.ServeFile(w, r, fp)
}

func enrichBlock(block *Block) {
	time.Sleep(500 * time.Millisecond)

	var extracted string
	switch block.BlockType {
	case "image":
		if block.FilePath != "" {
			out, err := exec.Command("tesseract", block.FilePath, "stdout", "-l", "eng").CombinedOutput()
			if err != nil {
				log.Printf("tesseract note for block %d: %v", block.ID, err)
				extracted = ""
			} else {
				extracted = strings.TrimSpace(string(out))
			}
		}
	case "pdf":
		if block.FilePath != "" {
			out, err := exec.Command("pdftotext", block.FilePath, "-").CombinedOutput()
			if err != nil {
				log.Printf("pdftotext note for block %d: %v", block.ID, err)
				extracted = ""
			} else {
				extracted = strings.TrimSpace(string(out))
			}
		}
	case "link":
		url := strings.TrimSpace(block.RawContent)
		extracted = fetchURLTitle(url)
	case "code":
		if block.FilePath != "" {
			data, err := os.ReadFile(block.FilePath)
			if err == nil {
				extracted = string(data)
				if len(extracted) > 5000 {
					extracted = extracted[:5000]
				}
			}
		} else {
			extracted = block.RawContent
		}
	default:
		extracted = block.RawContent
	}

	newSpace := block.Space
	if newSpace == "Unfiled" || newSpace == "" {
		newSpace = autoClassifySpace(extracted + " " + block.RawContent + " " + block.FileName)
	}

	_, err := db.Exec(`UPDATE blocks SET extracted_content = ?, space = ? WHERE id = ?`,
		extracted, newSpace, block.ID)
	if err != nil {
		log.Printf("enrich update error for block %d: %v", block.ID, err)
		return
	}

	updated, err := getBlockByID(block.ID)
	if err != nil {
		log.Printf("enrich fetch error for block %d: %v", block.ID, err)
		return
	}
	broadcast(WSEvent{Type: "BLOCK_UPDATED", Block: updated})
}

func fetchURLTitle(rawURL string) string {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(rawURL)
	if err != nil {
		return rawURL
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return rawURL
	}
	re := regexp.MustCompile(`(?i)<title[^>]*>(.*?)</title>`)
	matches := re.FindSubmatch(body)
	if len(matches) > 1 {
		return strings.TrimSpace(string(matches[1]))
	}
	return rawURL
}

func autoClassifySpace(text string) string {
	lower := strings.ToLower(text)

	dbmsKeywords := []string{"sql", "database", "query", "schema", "index", "table", "postgres", "mysql", "sqlite", "mongodb", "redis", "nosql", "orm", "migration", "join", "select", "insert"}
	netKeywords := []string{"tcp", "udp", "http", "socket", "network", "dns", "ip address", "routing", "firewall", "proxy", "latency", "bandwidth", "packet", "protocol", "rest api", "grpc", "websocket"}
	osKeywords := []string{"mutex", "semaphore", "process", "thread", "kernel", "syscall", "memory", "deadlock", "scheduler", "filesystem", "inode", "pipe", "signal", "fork", "virtual memory", "page fault"}
	codeKeywords := []string{"function", "class", "variable", "algorithm", "loop", "recursion", "data structure", "binary tree", "hash map", "stack", "queue", "linked list", "sorting", "graph", "dynamic programming"}
	researchKeywords := []string{"paper", "study", "research", "journal", "abstract", "methodology", "hypothesis", "experiment", "findings", "conclusion", "arxiv", "doi", "citation"}

	score := map[string]int{
		"DBMS":     0,
		"Networks": 0,
		"OS":       0,
		"Code":     0,
		"Research": 0,
	}
	for _, kw := range dbmsKeywords {
		if strings.Contains(lower, kw) {
			score["DBMS"]++
		}
	}
	for _, kw := range netKeywords {
		if strings.Contains(lower, kw) {
			score["Networks"]++
		}
	}
	for _, kw := range osKeywords {
		if strings.Contains(lower, kw) {
			score["OS"]++
		}
	}
	for _, kw := range codeKeywords {
		if strings.Contains(lower, kw) {
			score["Code"]++
		}
	}
	for _, kw := range researchKeywords {
		if strings.Contains(lower, kw) {
			score["Research"]++
		}
	}

	best := "Unfiled"
	bestScore := 0
	for k, v := range score {
		if v > bestScore {
			bestScore = v
			best = k
		}
	}
	if bestScore < 2 {
		return "Unfiled"
	}
	return best
}

func getLocalIPs() []string {
	var ips []string
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ips
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
			ips = append(ips, ipnet.IP.String())
		}
	}
	return ips
}

func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next(w, r)
	}
}

const desktopHTML = `<!DOCTYPE html>
<html lang="en" class="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>StreamSpace — Personal Resource Hub</title>
<meta name="description" content="StreamSpace is a local-first, cross-device personal resource space for collecting, organizing, and searching your files, links, notes, and code snippets.">
<script src="https://cdn.tailwindcss.com"></script>
<script>
tailwind.config = {
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        surface: { 50: '#f8fafc', 100: '#1e1b2e', 200: '#161325', 300: '#110e1f', 400: '#0c0a18', 900: '#080612', 950: '#050410' },
        accent: { 400: '#a78bfa', 500: '#8b5cf6', 600: '#7c3aed', 700: '#6d28d9' },
        neon: { green: '#4ade80', blue: '#60a5fa', pink: '#f472b6', amber: '#fbbf24' }
      },
      fontFamily: { sans: ['Inter', 'system-ui', 'sans-serif'] }
    }
  }
}
</script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
<style>
  ::-webkit-scrollbar { width: 6px; height: 6px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-thumb { background: #4c1d95; border-radius: 8px; }
  ::-webkit-scrollbar-thumb:hover { background: #7c3aed; }
  * { scrollbar-width: thin; scrollbar-color: #4c1d95 transparent; }
  body { font-family: 'Inter', system-ui, sans-serif; }
  .glass { background: rgba(30, 27, 46, 0.7); backdrop-filter: blur(16px); -webkit-backdrop-filter: blur(16px); border: 1px solid rgba(139, 92, 246, 0.15); }
  .glass-strong { background: rgba(22, 19, 37, 0.85); backdrop-filter: blur(24px); -webkit-backdrop-filter: blur(24px); border: 1px solid rgba(139, 92, 246, 0.2); }
  .card-glow:hover { box-shadow: 0 0 30px rgba(139, 92, 246, 0.15), 0 8px 32px rgba(0,0,0,0.3); }
  .sidebar-item { transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1); }
  .sidebar-item:hover { background: rgba(139, 92, 246, 0.15); transform: translateX(4px); }
  .sidebar-item.active { background: rgba(139, 92, 246, 0.25); border-left: 3px solid #8b5cf6; }
  .fade-in { animation: fadeIn 0.3s ease-out; }
  @keyframes fadeIn { from { opacity: 0; transform: translateY(8px); } to { opacity: 1; transform: translateY(0); } }
  @keyframes pulse-border { 0%, 100% { border-color: rgba(139, 92, 246, 0.3); } 50% { border-color: rgba(139, 92, 246, 0.7); } }
  .drop-active { animation: pulse-border 1s ease-in-out infinite; background: rgba(139, 92, 246, 0.08) !important; }
  .type-badge { font-size: 0.65rem; padding: 2px 8px; border-radius: 9999px; font-weight: 600; letter-spacing: 0.05em; text-transform: uppercase; }
  .chip { transition: all 0.15s ease; cursor: pointer; user-select: none; }
  .chip:hover { transform: scale(1.05); }
  .chip.active { background: #7c3aed; color: white; box-shadow: 0 0 12px rgba(139, 92, 246, 0.4); }
  .toast { position: fixed; bottom: 80px; right: 24px; z-index: 9999; padding: 12px 20px; border-radius: 12px; font-size: 0.85rem; font-weight: 500; opacity: 0; transform: translateY(12px); transition: all 0.3s ease; pointer-events: none; }
  .toast.show { opacity: 1; transform: translateY(0); }
</style>
</head>
<body class="bg-surface-950 text-gray-100 min-h-screen flex overflow-hidden">

<!-- Toast -->
<div id="toast" class="toast glass text-accent-400"></div>

<!-- Drag Overlay -->
<div id="dropOverlay" class="fixed inset-0 z-50 hidden items-center justify-center bg-surface-950/80 backdrop-blur-sm">
  <div class="flex flex-col items-center gap-4 p-12 rounded-3xl border-2 border-dashed border-accent-500/50 glass">
    <svg class="w-16 h-16 text-accent-400 animate-bounce" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12"/></svg>
    <p class="text-xl font-semibold text-accent-400">Drop files to upload</p>
    <p class="text-sm text-gray-400">Images, PDFs, code files, documents...</p>
  </div>
</div>

<!-- Sidebar -->
<aside id="sidebar" class="w-64 min-w-[256px] h-screen flex flex-col glass-strong border-r border-accent-700/20">
  <div class="p-5 border-b border-white/5">
    <div class="flex items-center gap-3">
      <div class="w-9 h-9 rounded-xl bg-gradient-to-br from-accent-500 to-purple-700 flex items-center justify-center shadow-lg shadow-accent-500/20">
        <svg class="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"/></svg>
      </div>
      <div>
        <h1 class="text-lg font-bold bg-gradient-to-r from-accent-400 to-purple-300 bg-clip-text text-transparent">StreamSpace</h1>
        <p class="text-[0.65rem] text-gray-500 font-medium tracking-wider uppercase">Resource Hub</p>
      </div>
    </div>
  </div>

  <div class="p-3">
    <p class="px-3 py-2 text-[0.65rem] font-semibold tracking-widest uppercase text-gray-500">Spaces</p>
    <nav id="spaceList" class="flex flex-col gap-0.5"></nav>
  </div>

  <div class="mt-auto p-4 border-t border-white/5">
    <div id="wsStatus" class="flex items-center gap-2 text-xs text-gray-500">
      <span class="w-2 h-2 rounded-full bg-red-500" id="wsStatusDot"></span>
      <span id="wsStatusText">Disconnected</span>
    </div>
    <div class="mt-2 text-[0.6rem] text-gray-600">Local-first &middot; Cross-device</div>
  </div>
</aside>

<!-- Main Content -->
<main class="flex-1 flex flex-col h-screen overflow-hidden">
  <!-- Top Bar -->
  <header class="glass-strong border-b border-accent-700/10 px-6 py-3 flex items-center gap-4 z-10">
    <div class="relative flex-1 max-w-xl">
      <svg class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/></svg>
      <input id="searchInput" type="text" placeholder="Search blocks..." class="w-full pl-10 pr-4 py-2.5 bg-surface-300/50 border border-white/5 rounded-xl text-sm text-gray-200 placeholder-gray-500 focus:outline-none focus:border-accent-500/50 focus:ring-1 focus:ring-accent-500/30 transition-all">
    </div>
    <div id="typeFilters" class="flex gap-2 flex-wrap">
      <button data-type="all" class="chip active px-3 py-1.5 rounded-lg text-xs font-medium bg-surface-200 text-gray-400 border border-white/5">All</button>
      <button data-type="text" class="chip px-3 py-1.5 rounded-lg text-xs font-medium bg-surface-200 text-gray-400 border border-white/5">Text</button>
      <button data-type="link" class="chip px-3 py-1.5 rounded-lg text-xs font-medium bg-surface-200 text-gray-400 border border-white/5">Links</button>
      <button data-type="image" class="chip px-3 py-1.5 rounded-lg text-xs font-medium bg-surface-200 text-gray-400 border border-white/5">Images</button>
      <button data-type="pdf" class="chip px-3 py-1.5 rounded-lg text-xs font-medium bg-surface-200 text-gray-400 border border-white/5">PDFs</button>
      <button data-type="code" class="chip px-3 py-1.5 rounded-lg text-xs font-medium bg-surface-200 text-gray-400 border border-white/5">Code</button>
    </div>
    <span id="blockCount" class="text-xs text-gray-500 font-mono ml-2">0 blocks</span>
  </header>

  <!-- Feed -->
  <div id="feed" class="flex-1 overflow-y-auto p-6">
    <div id="emptyState" class="hidden flex-col items-center justify-center h-full gap-4 text-center">
      <div class="w-20 h-20 rounded-2xl bg-surface-200 flex items-center justify-center">
        <svg class="w-10 h-10 text-gray-600" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4"/></svg>
      </div>
      <p class="text-gray-500 text-lg font-medium">No blocks yet</p>
      <p class="text-gray-600 text-sm">Paste, drag, or type to add your first resource</p>
    </div>
    <div id="blockGrid" class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4"></div>
  </div>

  <!-- Bottom Input Bar -->
  <footer class="glass-strong border-t border-accent-700/10 px-6 py-3">
    <div class="flex gap-3 items-center max-w-4xl mx-auto">
      <select id="inputSpace" class="bg-surface-200 border border-white/10 rounded-xl px-3 py-2.5 text-sm text-gray-300 focus:outline-none focus:border-accent-500/50 cursor-pointer">
        <option value="Unfiled">Unfiled</option>
        <option value="DBMS">DBMS</option>
        <option value="Networks">Networks</option>
        <option value="OS">OS</option>
        <option value="Code">Code</option>
        <option value="Research">Research</option>
      </select>
      <div class="flex-1 relative">
        <input id="textInput" type="text" placeholder="Type a note, paste a URL, or drop a file..." class="w-full px-4 py-2.5 bg-surface-300/50 border border-white/5 rounded-xl text-sm text-gray-200 placeholder-gray-500 focus:outline-none focus:border-accent-500/50 focus:ring-1 focus:ring-accent-500/30 transition-all pr-24">
        <div class="absolute right-2 top-1/2 -translate-y-1/2 flex gap-1">
          <label class="p-1.5 rounded-lg hover:bg-accent-500/20 cursor-pointer transition-colors" title="Upload file">
            <svg class="w-4 h-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15.172 7l-6.586 6.586a2 2 0 102.828 2.828l6.414-6.586a4 4 0 00-5.656-5.656l-6.415 6.585a6 6 0 108.486 8.486L20.5 13"/></svg>
            <input type="file" id="fileInput" class="hidden" multiple>
          </label>
        </div>
      </div>
      <button id="sendBtn" class="px-5 py-2.5 bg-gradient-to-r from-accent-600 to-accent-500 hover:from-accent-500 hover:to-accent-400 rounded-xl text-sm font-semibold text-white shadow-lg shadow-accent-500/20 transition-all hover:shadow-accent-500/40 hover:scale-[1.02] active:scale-95">
        Send
      </button>
    </div>
  </footer>
</main>

<!-- Space Edit Modal -->
<div id="spaceModal" class="fixed inset-0 z-50 hidden items-center justify-center bg-black/60 backdrop-blur-sm">
  <div class="glass-strong rounded-2xl p-6 w-full max-w-sm shadow-2xl">
    <h3 class="text-lg font-bold text-accent-400 mb-4">Move to Space</h3>
    <input type="hidden" id="modalBlockId">
    <div id="modalSpaceButtons" class="grid grid-cols-2 gap-2 mb-4"></div>
    <div class="flex gap-2">
      <input id="modalAnnotation" type="text" placeholder="Add a note (optional)..." class="flex-1 px-3 py-2 bg-surface-300/50 border border-white/10 rounded-xl text-sm text-gray-200 placeholder-gray-500 focus:outline-none focus:border-accent-500/50">
      <button id="modalSave" class="px-4 py-2 bg-accent-600 hover:bg-accent-500 rounded-xl text-sm font-semibold text-white transition-colors">Save</button>
    </div>
    <button id="modalClose" class="mt-3 w-full py-2 rounded-xl text-sm text-gray-400 hover:text-gray-200 hover:bg-surface-200 transition-all">Cancel</button>
  </div>
</div>

<script>
(function() {
  var state = {
    blocks: [],
    currentSpace: 'All',
    currentType: 'all',
    searchQuery: '',
    ws: null,
    wsReconnectTimer: null
  };

  var SPACES = ['All', 'Unfiled', 'DBMS', 'Networks', 'OS', 'Code', 'Research'];
  var TYPE_COLORS = {
    text: 'bg-emerald-500/20 text-emerald-400',
    link: 'bg-blue-500/20 text-blue-400',
    image: 'bg-pink-500/20 text-pink-400',
    pdf: 'bg-amber-500/20 text-amber-400',
    code: 'bg-cyan-500/20 text-cyan-400'
  };
  var TYPE_ICONS = {
    text: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"/>',
    link: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"/>',
    image: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z"/>',
    pdf: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 21h10a2 2 0 002-2V9.414a1 1 0 00-.293-.707l-5.414-5.414A1 1 0 0012.586 3H7a2 2 0 00-2 2v14a2 2 0 002 2z"/>',
    code: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4"/>'
  };

  function $(id) { return document.getElementById(id); }
  function showToast(msg, type) {
    var t = $('toast');
    t.textContent = msg;
    t.className = 'toast glass ' + (type === 'error' ? 'text-red-400 border-red-500/30' : 'text-accent-400');
    t.classList.add('show');
    setTimeout(function() { t.classList.remove('show'); }, 3000);
  }

  function renderSpaces() {
    var nav = $('spaceList');
    var spaceCounts = {};
    state.blocks.forEach(function(b) {
      spaceCounts[b.space] = (spaceCounts[b.space] || 0) + 1;
    });
    var allSpaces = SPACES.slice();
    state.blocks.forEach(function(b) {
      if (allSpaces.indexOf(b.space) === -1) { allSpaces.push(b.space); }
    });
    var html = '';
    allSpaces.forEach(function(sp) {
      var isActive = state.currentSpace === sp;
      var count = sp === 'All' ? state.blocks.length : (spaceCounts[sp] || 0);
      var iconMap = {
        'All': '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2V6zM14 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2V6zM4 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2zM14 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2v-2z"/>',
        'Unfiled': '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4"/>',
        'DBMS': '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4"/>',
        'Networks': '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9"/>',
        'OS': '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 3v2m6-2v2M9 19v2m6-2v2M5 9H3m2 6H3m18-6h-2m2 6h-2M7 19h10a2 2 0 002-2V7a2 2 0 00-2-2H7a2 2 0 00-2 2v10a2 2 0 002 2zM9 9h6v6H9V9z"/>',
        'Code': '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4"/>',
        'Research': '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253"/>'
      };
      var icon = iconMap[sp] || iconMap['Unfiled'];
      html += '<button onclick="window.setSpace(\'' + sp + '\')" class="sidebar-item flex items-center gap-3 px-3 py-2.5 rounded-xl text-sm ' + (isActive ? 'active text-accent-400 font-semibold' : 'text-gray-400 hover:text-gray-200') + '">' +
        '<svg class="w-4 h-4 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">' + icon + '</svg>' +
        '<span class="flex-1 text-left">' + sp + '</span>' +
        '<span class="text-[0.65rem] px-1.5 py-0.5 rounded-md ' + (isActive ? 'bg-accent-500/20 text-accent-400' : 'bg-white/5 text-gray-500') + '">' + count + '</span>' +
      '</button>';
    });
    nav.innerHTML = html;
  }

  function renderBlocks(blocks) {
    var grid = $('blockGrid');
    var empty = $('emptyState');
    $('blockCount').textContent = blocks.length + ' block' + (blocks.length !== 1 ? 's' : '');
    if (blocks.length === 0) {
      grid.innerHTML = '';
      empty.style.display = 'flex';
      empty.className = 'flex flex-col items-center justify-center h-full gap-4 text-center';
      return;
    }
    empty.style.display = 'none';
    empty.className = 'hidden';
    var html = '';
    blocks.forEach(function(b) {
      var typeColor = TYPE_COLORS[b.block_type] || TYPE_COLORS.text;
      var typeIcon = TYPE_ICONS[b.block_type] || TYPE_ICONS.text;
      var preview = '';
      if (b.block_type === 'image' && b.file_path) {
        preview = '<div class="mt-3 rounded-lg overflow-hidden border border-white/5"><img src="/api/file?path=' + encodeURIComponent(b.file_path) + '" alt="' + escapeHtml(b.file_name) + '" class="w-full h-40 object-cover" onerror="this.parentNode.style.display=\'none\'"></div>';
      } else if (b.block_type === 'link') {
        preview = '<a href="' + escapeHtml(b.raw_content) + '" target="_blank" rel="noopener" class="mt-3 block text-sm text-blue-400 hover:text-blue-300 truncate transition-colors">' + escapeHtml(b.raw_content) + '</a>';
      } else if (b.block_type === 'code') {
        var codeText = b.extracted_content || b.raw_content || '';
        if (codeText.length > 300) { codeText = codeText.substring(0, 300) + '...'; }
        preview = '<pre class="mt-3 p-3 bg-surface-400/50 rounded-lg text-xs text-gray-300 overflow-x-auto border border-white/5 font-mono leading-relaxed max-h-32 overflow-y-auto">' + escapeHtml(codeText) + '</pre>';
      }

      var extractedPreview = '';
      if (b.extracted_content && b.block_type !== 'code') {
        var et = b.extracted_content;
        if (et.length > 180) { et = et.substring(0, 180) + '...'; }
        extractedPreview = '<p class="mt-2 text-xs text-gray-500 leading-relaxed line-clamp-3">' + escapeHtml(et) + '</p>';
      }

      var annotationBadge = '';
      if (b.annotation) {
        annotationBadge = '<div class="mt-2 flex items-center gap-1.5"><svg class="w-3 h-3 text-accent-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 8h10M7 12h4m1 8l-4-4H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-3l-4 4z"/></svg><span class="text-xs text-accent-400/80">' + escapeHtml(b.annotation) + '</span></div>';
      }

      var displayName = b.file_name || (b.raw_content ? b.raw_content.substring(0, 60) : 'Untitled');
      var ago = timeAgo(b.created_at);
      var fileInfo = '';
      if (b.file_size > 0) {
        fileInfo = '<span class="text-gray-600">&middot;</span><span>' + formatBytes(b.file_size) + '</span>';
      }

      html += '<div class="glass rounded-2xl p-5 card-glow transition-all duration-300 fade-in group" data-id="' + b.id + '">' +
        '<div class="flex items-start justify-between gap-3">' +
          '<div class="flex items-center gap-2.5 min-w-0">' +
            '<div class="w-8 h-8 rounded-lg ' + typeColor + ' flex items-center justify-center flex-shrink-0">' +
              '<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">' + typeIcon + '</svg>' +
            '</div>' +
            '<div class="min-w-0">' +
              '<h3 class="text-sm font-semibold text-gray-200 truncate">' + escapeHtml(displayName) + '</h3>' +
              '<div class="flex items-center gap-1.5 text-[0.65rem] text-gray-500 mt-0.5">' +
                '<span class="type-badge ' + typeColor + '">' + b.block_type + '</span>' +
                '<span class="text-gray-600">&middot;</span>' +
                '<span>' + ago + '</span>' +
                fileInfo +
              '</div>' +
            '</div>' +
          '</div>' +
          '<div class="flex gap-1 opacity-0 group-hover:opacity-100 transition-opacity">' +
            '<button onclick="window.openSpaceModal(' + b.id + ')" class="p-1.5 rounded-lg hover:bg-accent-500/20 transition-colors" title="Move to space">' +
              '<svg class="w-3.5 h-3.5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 21h10a2 2 0 002-2V9.414a1 1 0 00-.293-.707l-5.414-5.414A1 1 0 0012.586 3H7a2 2 0 00-2 2v14a2 2 0 002 2z"/></svg>' +
            '</button>' +
            '<button onclick="window.copyBlock(' + b.id + ')" class="p-1.5 rounded-lg hover:bg-accent-500/20 transition-colors" title="Copy content">' +
              '<svg class="w-3.5 h-3.5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"/></svg>' +
            '</button>' +
            '<button onclick="window.deleteBlock(' + b.id + ')" class="p-1.5 rounded-lg hover:bg-red-500/20 transition-colors" title="Delete">' +
              '<svg class="w-3.5 h-3.5 text-gray-400 hover:text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/></svg>' +
            '</button>' +
          '</div>' +
        '</div>' +
        '<div class="flex items-center gap-1.5 mt-2.5">' +
          '<span class="px-2 py-0.5 rounded-md bg-accent-500/10 text-accent-400 text-[0.65rem] font-medium">' + escapeHtml(b.space) + '</span>' +
        '</div>' +
        preview +
        extractedPreview +
        annotationBadge +
      '</div>';
    });
    grid.innerHTML = html;
  }

  function escapeHtml(str) {
    if (!str) return '';
    return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#039;');
  }

  function timeAgo(dateStr) {
    if (!dateStr) return '';
    var now = new Date();
    var d = new Date(dateStr);
    var diff = Math.floor((now - d) / 1000);
    if (diff < 60) return 'just now';
    if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
    if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
    if (diff < 604800) return Math.floor(diff / 86400) + 'd ago';
    return d.toLocaleDateString();
  }

  function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    var k = 1024;
    var sizes = ['B', 'KB', 'MB', 'GB'];
    var i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
  }

  function fetchBlocks() {
    var params = [];
    if (state.searchQuery) params.push('q=' + encodeURIComponent(state.searchQuery));
    if (state.currentSpace && state.currentSpace !== 'All') params.push('space=' + encodeURIComponent(state.currentSpace));
    if (state.currentType && state.currentType !== 'all') params.push('type=' + encodeURIComponent(state.currentType));
    var url = '/api/blocks' + (params.length ? '?' + params.join('&') : '');

    fetch(url)
      .then(function(r) { return r.json(); })
      .then(function(data) {
        state.blocks = data || [];
        renderSpaces();
        renderBlocks(state.blocks);
      })
      .catch(function(err) {
        console.error('fetchBlocks error:', err);
        showToast('Failed to fetch blocks', 'error');
      });
  }

  function uploadFile(file, space) {
    var fd = new FormData();
    fd.append('file', file);
    fd.append('space', space || $('inputSpace').value);
    fetch('/api/blocks', { method: 'POST', body: fd })
      .then(function(r) {
        if (!r.ok) {
          return r.text().then(function(t) { throw new Error(t); });
        }
        return r.json();
      })
      .then(function(data) {
        showToast('Uploaded: ' + (data.file_name || 'file'));
        fetchBlocks();
      })
      .catch(function(err) {
        console.error('upload error:', err);
        showToast('Upload failed: ' + err.message, 'error');
      });
  }

  function uploadText(text, space) {
    var fd = new FormData();
    fd.append('raw_content', text);
    fd.append('space', space || $('inputSpace').value);
    fetch('/api/blocks', { method: 'POST', body: fd })
      .then(function(r) {
        if (!r.ok) {
          return r.text().then(function(t) { throw new Error(t); });
        }
        return r.json();
      })
      .then(function(data) {
        showToast('Saved note');
        fetchBlocks();
      })
      .catch(function(err) {
        console.error('text upload error:', err);
        showToast('Save failed: ' + err.message, 'error');
      });
  }

  function connectWS() {
    if (state.ws && state.ws.readyState <= 1) return;
    var protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    var wsUrl = protocol + '//' + window.location.host + '/ws';
    state.ws = new WebSocket(wsUrl);
    state.ws.onopen = function() {
      $('wsStatusDot').className = 'w-2 h-2 rounded-full bg-emerald-500 shadow-lg shadow-emerald-500/50';
      $('wsStatusText').textContent = 'Connected';
      if (state.wsReconnectTimer) { clearTimeout(state.wsReconnectTimer); state.wsReconnectTimer = null; }
    };
    state.ws.onmessage = function(e) {
      try {
        var event = JSON.parse(e.data);
        if (event.type === 'BLOCK_CREATED' || event.type === 'BLOCK_UPDATED' || event.type === 'BLOCK_DELETED') {
          fetchBlocks();
        }
      } catch(err) {
        console.error('ws parse error:', err);
      }
    };
    state.ws.onclose = function() {
      $('wsStatusDot').className = 'w-2 h-2 rounded-full bg-red-500';
      $('wsStatusText').textContent = 'Disconnected';
      state.wsReconnectTimer = setTimeout(connectWS, 3000);
    };
    state.ws.onerror = function() {
      if (state.ws) state.ws.close();
    };
  }

  window.setSpace = function(sp) {
    state.currentSpace = sp;
    fetchBlocks();
  };

  window.deleteBlock = function(id) {
    if (!confirm('Delete this block?')) return;
    fetch('/api/blocks/delete?id=' + id, { method: 'DELETE' })
      .then(function() { showToast('Block deleted'); fetchBlocks(); })
      .catch(function(err) { showToast('Delete failed', 'error'); });
  };

  window.copyBlock = function(id) {
    var block = state.blocks.find(function(b) { return b.id === id; });
    if (!block) return;
    var text = block.raw_content || block.extracted_content || '';
    navigator.clipboard.writeText(text).then(function() {
      showToast('Copied to clipboard');
    }).catch(function() {
      showToast('Copy failed', 'error');
    });
  };

  window.openSpaceModal = function(id) {
    $('modalBlockId').value = id;
    $('modalAnnotation').value = '';
    var block = state.blocks.find(function(b) { return b.id === id; });
    if (block && block.annotation) { $('modalAnnotation').value = block.annotation; }
    var btns = '';
    SPACES.forEach(function(sp) {
      if (sp === 'All') return;
      var isActive = block && block.space === sp;
      btns += '<button onclick="window.selectModalSpace(this, \'' + sp + '\')" class="px-3 py-2 rounded-xl text-sm font-medium transition-all border ' + (isActive ? 'bg-accent-600 border-accent-500 text-white' : 'bg-surface-200 border-white/5 text-gray-400 hover:border-accent-500/30 hover:text-gray-200') + '" data-space="' + sp + '">' + sp + '</button>';
    });
    $('modalSpaceButtons').innerHTML = btns;
    var modal = $('spaceModal');
    modal.style.display = 'flex';
    modal.className = 'fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm';
  };

  window.selectModalSpace = function(btn, space) {
    var buttons = $('modalSpaceButtons').querySelectorAll('button');
    buttons.forEach(function(b) {
      b.className = 'px-3 py-2 rounded-xl text-sm font-medium transition-all border bg-surface-200 border-white/5 text-gray-400 hover:border-accent-500/30 hover:text-gray-200';
    });
    btn.className = 'px-3 py-2 rounded-xl text-sm font-medium transition-all border bg-accent-600 border-accent-500 text-white';
  };

  $('modalSave').addEventListener('click', function() {
    var id = parseInt($('modalBlockId').value);
    var activeBtn = $('modalSpaceButtons').querySelector('.bg-accent-600');
    var space = activeBtn ? activeBtn.getAttribute('data-space') : 'Unfiled';
    var annotation = $('modalAnnotation').value;
    fetch('/api/blocks/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id: id, space: space, annotation: annotation })
    }).then(function(r) {
      if (!r.ok) throw new Error('Update failed');
      showToast('Block updated');
      $('spaceModal').style.display = 'none';
      $('spaceModal').className = 'fixed inset-0 z-50 hidden items-center justify-center bg-black/60 backdrop-blur-sm';
      fetchBlocks();
    }).catch(function() { showToast('Update failed', 'error'); });
  });

  $('modalClose').addEventListener('click', function() {
    $('spaceModal').style.display = 'none';
    $('spaceModal').className = 'fixed inset-0 z-50 hidden items-center justify-center bg-black/60 backdrop-blur-sm';
  });

  var searchDebounce = null;
  $('searchInput').addEventListener('input', function(e) {
    clearTimeout(searchDebounce);
    searchDebounce = setTimeout(function() {
      state.searchQuery = e.target.value.trim();
      fetchBlocks();
    }, 300);
  });

  document.querySelectorAll('#typeFilters .chip').forEach(function(chip) {
    chip.addEventListener('click', function() {
      document.querySelectorAll('#typeFilters .chip').forEach(function(c) { c.classList.remove('active'); });
      chip.classList.add('active');
      state.currentType = chip.getAttribute('data-type');
      fetchBlocks();
    });
  });

  $('sendBtn').addEventListener('click', function() {
    var text = $('textInput').value.trim();
    if (!text) return;
    uploadText(text, $('inputSpace').value);
    $('textInput').value = '';
  });

  $('textInput').addEventListener('keydown', function(e) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      $('sendBtn').click();
    }
  });

  $('fileInput').addEventListener('change', function(e) {
    Array.from(e.target.files).forEach(function(f) { uploadFile(f); });
    e.target.value = '';
  });

  window.addEventListener('paste', function(e) {
    var activeTag = document.activeElement ? document.activeElement.tagName : '';
    if (activeTag === 'INPUT' || activeTag === 'TEXTAREA') {
      return;
    }

    var items = e.clipboardData && e.clipboardData.items;
    if (items) {
      for (var i = 0; i < items.length; i++) {
        if (items[i].kind === 'file') {
          e.preventDefault();
          var file = items[i].getAsFile();
          if (file) {
            uploadFile(file);
            return;
          }
        }
      }
    }

    var text = e.clipboardData ? e.clipboardData.getData('text') : '';
    if (text && text.trim().length > 0) {
      e.preventDefault();
      uploadText(text.trim());
    }
  });

  var dragCounter = 0;
  document.body.addEventListener('dragenter', function(e) {
    e.preventDefault();
    dragCounter++;
    $('dropOverlay').style.display = 'flex';
    $('dropOverlay').className = 'fixed inset-0 z-50 flex items-center justify-center bg-surface-950/80 backdrop-blur-sm';
  });
  document.body.addEventListener('dragleave', function(e) {
    e.preventDefault();
    dragCounter--;
    if (dragCounter <= 0) {
      dragCounter = 0;
      $('dropOverlay').style.display = 'none';
      $('dropOverlay').className = 'fixed inset-0 z-50 hidden items-center justify-center bg-surface-950/80 backdrop-blur-sm';
    }
  });
  document.body.addEventListener('dragover', function(e) { e.preventDefault(); });
  document.body.addEventListener('drop', function(e) {
    e.preventDefault();
    dragCounter = 0;
    $('dropOverlay').style.display = 'none';
    $('dropOverlay').className = 'fixed inset-0 z-50 hidden items-center justify-center bg-surface-950/80 backdrop-blur-sm';
    if (e.dataTransfer && e.dataTransfer.files.length > 0) {
      Array.from(e.dataTransfer.files).forEach(function(f) { uploadFile(f); });
    }
  });

  fetchBlocks();
  connectWS();
})();
</script>
</body>
</html>`

func main() {
	initDB()

	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(desktopHTML))
	})

	mux.HandleFunc("/api/blocks", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			corsMiddleware(handleGetBlocks)(w, r)
		case http.MethodPost:
			corsMiddleware(handleCreateBlock)(w, r)
		case http.MethodOptions:
			corsMiddleware(func(w http.ResponseWriter, r *http.Request) {})(w, r)
		default:
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/blocks/update", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		handleUpdateBlock(w, r)
	}))

	mux.HandleFunc("/api/blocks/delete", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		handleDeleteBlock(w, r)
	}))

	mux.HandleFunc("/api/file", corsMiddleware(handleServeFile))
	mux.HandleFunc("/ws", handleWS)

	port := "8080"
	ips := getLocalIPs()
	fmt.Println("╔══════════════════════════════════════════════════════════╗")
	fmt.Println("║              StreamSpace — Resource Hub                 ║")
	fmt.Println("╠══════════════════════════════════════════════════════════╣")
	fmt.Printf("║  Local:   http://localhost:%s                        ║\n", port)
	for _, ip := range ips {
		line := fmt.Sprintf("║  Network: http://%s:%s", ip, port)
		for len(line) < 59 {
			line += " "
		}
		line += "║"
		fmt.Println(line)
	}
	fmt.Println("╠══════════════════════════════════════════════════════════╣")
	fmt.Println("║  Upload: paste, drag-drop, or type in browser          ║")
	fmt.Println("║  Mobile: connect Flutter app to this server            ║")
	fmt.Println("╚══════════════════════════════════════════════════════════╝")

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
	log.Fatal(server.ListenAndServe())
}
