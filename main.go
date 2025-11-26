package main

import (
    "bufio"
    "encoding/json"
    "fmt"
    "os"
    "sort"
    "strings"
    "time"
)

// Entry represents a value with optional TTL
type Entry struct {
    Value     string
    ExpiresAt *time.Time // nil means no expiration
}

// WriteOp represents a buffered write operation during a transaction
type WriteOp struct {
    Type      string // "SET", "DEL", "EXPIRE", "PERSIST"
    Value     string
    ExpiresAt *time.Time
}

// Database is the main key-value store with transaction support
type Database struct {
    // Use a sorted slice for ordered storage (as required - no built-in map for core store)
    keys   []string
    values map[string]Entry // We can use a map for O(1) lookups alongside sorted keys
    
    // Transaction state
    inTransaction bool
    pendingWrites map[string]*WriteOp
    
    // Persistence
    logFile *os.File
}

// NewDatabase creates a new database instance
func NewDatabase() (*Database, error) {
    db := &Database{
        keys:          make([]string, 0),
        values:        make(map[string]Entry),
        pendingWrites: make(map[string]*WriteOp),
    }
    
    // Open log file in append mode
    logFile, err := os.OpenFile("data.db", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
    if err != nil {
        return nil, fmt.Errorf("failed to open log file: %w", err)
    }
    db.logFile = logFile
    
    // Replay existing log
    if err := db.replayLog(); err != nil {
        return nil, fmt.Errorf("failed to replay log: %w", err)
    }
    
    return db, nil
}

// Close shuts down the database
func (db *Database) Close() error {
    if db.logFile != nil {
        return db.logFile.Close()
    }
    return nil
}

// replayLog rebuilds state from the append-only log
func (db *Database) replayLog() error {
    file, err := os.Open("data.db")
    if os.IsNotExist(err) {
        return nil // No existing log
    }
    if err != nil {
        return err
    }
    defer file.Close()
    
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        line := scanner.Text()
        if err := db.applyLogEntry(line); err != nil {
            // Continue past corrupted entries
            continue
        }
    }
    return scanner.Err()
}

// applyLogEntry applies a single log entry
func (db *Database) applyLogEntry(line string) error {
    parts := strings.Fields(line)
    if len(parts) < 2 {
        return fmt.Errorf("invalid log entry")
    }
    
    cmd := parts[0]
    key := parts[1]
    
    switch cmd {
    case "SET":
        if len(parts) < 3 {
            return fmt.Errorf("invalid SET entry")
        }
        value := parts[2]
        db.set(key, value, nil)
        
    case "DEL":
        db.del(key)
        
    case "EXPIRE":
        if len(parts) < 3 {
            return fmt.Errorf("invalid EXPIRE entry")
        }
        var expiresAt time.Time
        if err := json.Unmarshal([]byte(parts[2]), &expiresAt); err != nil {
            return err
        }
        if entry, exists := db.values[key]; exists {
            entry.ExpiresAt = &expiresAt
            db.values[key] = entry
        }
        
    case "PERSIST":
        if entry, exists := db.values[key]; exists {
            entry.ExpiresAt = nil
            db.values[key] = entry
        }
    }
    
    return nil
}

// logWrite appends a write operation to the log
func (db *Database) logWrite(format string, args ...interface{}) error {
    entry := fmt.Sprintf(format, args...) + "\n"
    _, err := db.logFile.WriteString(entry)
    if err != nil {
        return err
    }
    return db.logFile.Sync()
}

// get retrieves a value, checking expiry
func (db *Database) get(key string) (string, bool) {
    entry, exists := db.values[key]
    if !exists {
        return "", false
    }
    
    // Check TTL
    if entry.ExpiresAt != nil && time.Now().After(*entry.ExpiresAt) {
        return "", false
    }
    
    return entry.Value, true
}

// set stores a value with optional expiry
func (db *Database) set(key, value string, expiresAt *time.Time) {
    // Update or add key
    if _, exists := db.values[key]; !exists {
        // Insert key in sorted order
        i := sort.SearchStrings(db.keys, key)
        db.keys = append(db.keys, "")
        copy(db.keys[i+1:], db.keys[i:])
        db.keys[i] = key
    }
    
    db.values[key] = Entry{Value: value, ExpiresAt: expiresAt}
}

// del removes a key
func (db *Database) del(key string) bool {
    if _, exists := db.values[key]; !exists {
        return false
    }
    
    // Remove from sorted keys
    i := sort.SearchStrings(db.keys, key)
    if i < len(db.keys) && db.keys[i] == key {
        db.keys = append(db.keys[:i], db.keys[i+1:]...)
    }
    
    delete(db.values, key)
    return true
}

// isExpired checks if a key is expired
func (db *Database) isExpired(key string) bool {
    entry, exists := db.values[key]
    if !exists {
        return true
    }
    if entry.ExpiresAt == nil {
        return false
    }
    return time.Now().After(*entry.ExpiresAt)
}

// exists checks if a key exists and is not expired
func (db *Database) exists(key string) bool {
    _, ok := db.get(key)
    return ok
}

// getRemainingTTL returns remaining milliseconds
func (db *Database) getRemainingTTL(key string) int64 {
    entry, exists := db.values[key]
    if !exists {
        return -2 // Key doesn't exist
    }
    
    if entry.ExpiresAt == nil {
        return -1 // No TTL
    }
    
    if db.isExpired(key) {
        return -2 // Expired
    }
    
    remaining := time.Until(*entry.ExpiresAt).Milliseconds()
    if remaining < 0 {
        return 0
    }
    return remaining
}

// begin starts a transaction
func (db *Database) begin() {
    db.inTransaction = true
    db.pendingWrites = make(map[string]*WriteOp)
}

// commit applies pending writes
func (db *Database) commit() error {
    if !db.inTransaction {
        return fmt.Errorf("no transaction in progress")
    }
    
    // Apply all pending writes
    for key, op := range db.pendingWrites {
        switch op.Type {
        case "SET":
            db.set(key, op.Value, op.ExpiresAt)
            if err := db.logWrite("SET %s %s", key, op.Value); err != nil {
                return err
            }
        case "DEL":
            db.del(key)
            if err := db.logWrite("DEL %s", key); err != nil {
                return err
            }
        case "EXPIRE":
            if entry, exists := db.values[key]; exists {
                entry.ExpiresAt = op.ExpiresAt
                db.values[key] = entry
                expiresAtBytes, _ := json.Marshal(op.ExpiresAt)
                if err := db.logWrite("EXPIRE %s %s", key, string(expiresAtBytes)); err != nil {
                    return err
                }
            }
        case "PERSIST":
            if entry, exists := db.values[key]; exists {
                entry.ExpiresAt = nil
                db.values[key] = entry
                if err := db.logWrite("PERSIST %s", key); err != nil {
                    return err
                }
            }
        }
    }
    
    // Clear transaction state
    db.inTransaction = false
    db.pendingWrites = make(map[string]*WriteOp)
    
    return nil
}

// abort discards pending writes
func (db *Database) abort() {
    db.inTransaction = false
    db.pendingWrites = make(map[string]*WriteOp)
}

// queueWrite buffers a write operation during transaction
func (db *Database) queueWrite(key string, op *WriteOp) {
    db.pendingWrites[key] = op
}

// getFromStoreOrTxn reads from transaction buffer or main store
func (db *Database) getFromStoreOrTxn(key string) (string, bool) {
    // Check transaction buffer first
    if db.inTransaction {
        if op, exists := db.pendingWrites[key]; exists {
            switch op.Type {
            case "SET":
                return op.Value, true
            case "DEL":
                return "", false
            }
        }
    }
    
    // Fall back to main store
    return db.get(key)
}

// existsFromStoreOrTxn checks existence with transaction awareness
func (db *Database) existsFromStoreOrTxn(key string) bool {
    if db.inTransaction {
        if op, exists := db.pendingWrites[key]; exists {
            switch op.Type {
            case "SET", "EXPIRE":
                return true
            case "DEL":
                return false
            }
        }
    }
    return db.exists(key)
}

// rangeKeys returns keys in lexicographic order within bounds
func (db *Database) rangeKeys(start, end string) []string {
    var result []string
    
    for _, key := range db.keys {
        // Check if key is within bounds
        inRange := true
        if start != "" && key < start {
            inRange = false
        }
        if end != "" && key > end {
            inRange = false
        }
        
        if inRange {
            // Only return non-expired keys
            if db.exists(key) {
                result = append(result, key)
            }
        }
    }
    
    return result
}

// command handlers
func handleSet(db *Database, args []string) string {
    if len(args) < 2 {
        return "ERR wrong number of arguments for SET"
    }
    key, value := args[0], args[1]
    
    if db.inTransaction {
        // In transaction, queue the write
        db.queueWrite(key, &WriteOp{
            Type:  "SET",
            Value: value,
        })
    } else {
        // Direct write
        db.set(key, value, nil)
        if err := db.logWrite("SET %s %s", key, value); err != nil {
            return "ERR write failed"
        }
    }
    
    return "OK"
}

func handleGet(db *Database, args []string) string {
    if len(args) < 1 {
        return "ERR wrong number of arguments for GET"
    }
    key := args[0]
    
    if value, exists := db.getFromStoreOrTxn(key); exists {
        return value
    }
    return "nil"
}

func handleDel(db *Database, args []string) string {
    if len(args) < 1 {
        return "ERR wrong number of arguments for DEL"
    }
    key := args[0]
    
    if db.inTransaction {
        // Queue for transaction
        db.queueWrite(key, &WriteOp{Type: "DEL"})
        return "1" // Assume it exists for now
    }
    
    if db.del(key) {
        if err := db.logWrite("DEL %s", key); err != nil {
            return "0"
        }
        return "1"
    }
    return "0"
}

func handleExists(db *Database, args []string) string {
    if len(args) < 1 {
        return "ERR wrong number of arguments for EXISTS"
    }
    key := args[0]
    
    if db.existsFromStoreOrTxn(key) {
        return "1"
    }
    return "0"
}

func handleMset(db *Database, args []string) string {
    if len(args) < 2 || len(args)%2 != 0 {
        return "ERR wrong number of arguments for MSET"
    }
    
    for i := 0; i < len(args); i += 2 {
        key, value := args[i], args[i+1]
        if db.inTransaction {
            db.queueWrite(key, &WriteOp{Type: "SET", Value: value})
        } else {
            db.set(key, value, nil)
            if err := db.logWrite("SET %s %s", key, value); err != nil {
                return "ERR write failed"
            }
        }
    }
    
    return "OK"
}

func handleMget(db *Database, args []string) []string {
    if len(args) < 1 {
        return []string{"ERR wrong number of arguments for MGET"}
    }
    
    results := make([]string, len(args))
    for i, key := range args {
        if value, exists := db.getFromStoreOrTxn(key); exists {
            results[i] = value
        } else {
            results[i] = "nil"
        }
    }
    return results
}

func handleBegin(db *Database, args []string) string {
    if db.inTransaction {
        return "ERR transaction already in progress"
    }
    db.begin()
    return "OK"
}

func handleCommit(db *Database, args []string) string {
    if err := db.commit(); err != nil {
        return fmt.Sprintf("ERR %s", err.Error())
    }
    return "OK"
}

func handleAbort(db *Database, args []string) string {
    if !db.inTransaction {
        return "ERR no transaction in progress"
    }
    db.abort()
    return "OK"
}

func handleExpire(db *Database, args []string) string {
    if len(args) < 2 {
        return "ERR wrong number of arguments for EXPIRE"
    }
    key := args[0]
    var ms int64
    if _, err := fmt.Sscanf(args[1], "%d", &ms); err != nil {
        return "ERR invalid milliseconds"
    }
    
    // Check if key exists (considering pending transactions)
    if !db.existsFromStoreOrTxn(key) {
        return "0"
    }
    
    // Handle immediate expiration
    if ms <= 0 {
        if db.inTransaction {
            db.queueWrite(key, &WriteOp{Type: "DEL"})
        } else {
            db.del(key)
            db.logWrite("DEL %s", key)
        }
        return "1"
    }
    
    // Set expiry
    expiresAt := time.Now().Add(time.Duration(ms) * time.Millisecond)
    
    if db.inTransaction {
        db.queueWrite(key, &WriteOp{
            Type:      "EXPIRE",
            ExpiresAt: &expiresAt,
        })
    } else {
        if entry, exists := db.values[key]; exists {
            entry.ExpiresAt = &expiresAt
            db.values[key] = entry
            expiresAtBytes, _ := json.Marshal(&expiresAt)
            db.logWrite("EXPIRE %s %s", key, string(expiresAtBytes))
        }
    }
    
    return "1"
}

func handleTTL(db *Database, args []string) string {
    if len(args) < 1 {
        return "ERR wrong number of arguments for TTL"
    }
    key := args[0]
    
    // Check transaction buffer first
    if db.inTransaction {
        if op, exists := db.pendingWrites[key]; exists {
            if op.Type == "DEL" {
                return "-2"
            }
            if op.Type == "SET" || op.Type == "EXPIRE" {
                if op.ExpiresAt != nil {
                    remaining := time.Until(*op.ExpiresAt).Milliseconds()
                    if remaining < 0 {
                        return "0"
                    }
                    return fmt.Sprintf("%d", remaining)
                }
                return "-1"
            }
        }
    }
    
    // Check main store
    return fmt.Sprintf("%d", db.getRemainingTTL(key))
}

func handlePersist(db *Database, args []string) string {
    if len(args) < 1 {
        return "ERR wrong number of arguments for PERSIST"
    }
    key := args[0]
    
    // Check transaction buffer first
    if db.inTransaction {
        if op, exists := db.pendingWrites[key]; exists {
            if op.Type == "DEL" {
                return "0"
            }
            hasTTL := op.ExpiresAt != nil || db.values[key].ExpiresAt != nil
            if hasTTL {
                op.ExpiresAt = nil
                op.Type = "PERSIST"
                return "1"
            }
            return "0"
        }
    }
    
    // Check main store
    entry, exists := db.values[key]
    if !exists || entry.ExpiresAt == nil {
        return "0"
    }
    
    if db.inTransaction {
        db.queueWrite(key, &WriteOp{Type: "PERSIST"})
    } else {
        entry.ExpiresAt = nil
        db.values[key] = entry
        db.logWrite("PERSIST %s", key)
    }
    
    return "1"
}

func handleRange(db *Database, args []string) []string {
    if len(args) < 2 {
        return []string{"ERR wrong number of arguments for RANGE"}
    }
    start, end := args[0], args[1]
    
    // Adjust empty strings for open bounds
    if start == "\"\"" {
        start = ""
    }
    if end == "\"\"" {
        end = ""
    }
    
    keys := db.rangeKeys(start, end)
    result := make([]string, 0, len(keys)+1)
    result = append(result, keys...)
    result = append(result, "END")
    return result
}

// processCommand handles incoming commands
func processCommand(db *Database, line string) []string {
    parts := strings.Fields(line)
    if len(parts) == 0 {
        return []string{""}
    }
    
    cmd := strings.ToUpper(parts[0])
    args := parts[1:]
    
    switch cmd {
    case "SET":
        return []string{handleSet(db, args)}
    case "GET":
        return []string{handleGet(db, args)}
    case "DEL":
        return []string{handleDel(db, args)}
    case "EXISTS":
        return []string{handleExists(db, args)}
    case "MSET":
        return []string{handleMset(db, args)}
    case "MGET":
        return handleMget(db, args)
    case "BEGIN":
        return []string{handleBegin(db, args)}
    case "COMMIT":
        return []string{handleCommit(db, args)}
    case "ABORT":
        return []string{handleAbort(db, args)}
    case "EXPIRE":
        return []string{handleExpire(db, args)}
    case "TTL":
        return []string{handleTTL(db, args)}
    case "PERSIST":
        return []string{handlePersist(db, args)}
    case "RANGE":
        return handleRange(db, args)
    case "EXIT":
        return []string{"OK"}
    default:
        return []string{"ERR unknown command '" + cmd + "'"}
    }
}

func main() {
    db, err := NewDatabase()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Failed to initialize database: %v\n", err)
        os.Exit(1)
    }
    defer db.Close()
    
    scanner := bufio.NewScanner(os.Stdin)
    for scanner.Scan() {
        line := scanner.Text()
        responses := processCommand(db, line)
        
        for _, response := range responses {
            fmt.Println(response)
        }
        
        if strings.ToUpper(strings.Fields(line)[0]) == "EXIT" {
            break
        }
    }
    
    if err := scanner.Err(); err != nil {
        fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
        os.Exit(1)
    }
}