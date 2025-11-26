#!/usr/bin/env python3
import sys
import time
import bisect
from typing import Dict, List, Tuple, Optional, Any

class KVStore:
    def __init__(self):
        self.data = []  # List of (key, value, ttl) tuples, maintained in sorted order by key
        self.transaction_buffer = None  # List of (operation, args) for current transaction
        self.log_file = "data.db"
        
        # Replay log on startup
        self._replay_log()
    
    def _find_key_index(self, key: str) -> int:
        """Binary search to find the index of a key, returns -1 if not found"""
        keys = [item[0] for item in self.data]
        lo, hi = 0, len(keys) - 1
        
        while lo <= hi:
            mid = (lo + hi) // 2
            if keys[mid] == key:
                return mid
            elif keys[mid] < key:
                lo = mid + 1
            else:
                hi = mid - 1
        return -1
    
    def _is_expired(self, index: int) -> bool:
        """Check if key at given index is expired"""
        if index < 0 or index >= len(self.data):
            return True
        
        _, _, ttl = self.data[index]
        if ttl is not None and time.time() * 1000 > ttl:
            # Remove expired key
            self.data.pop(index)
            return True
        return False
    
    def _get_key_index(self, key: str, check_expired: bool = True) -> int:
        """Get index of key, handling expiration"""
        index = self._find_key_index(key)
        if index == -1:
            return -1
        
        if check_expired and self._is_expired(index):
            return -1
        return index
    
    def _set_key(self, key: str, value: str, ttl: Optional[float] = None) -> bool:
        """Internal method to set a key-value pair"""
        index = self._find_key_index(key)
        
        if index != -1:
            # Update existing key
            current_ttl = self.data[index][2]
            new_ttl = ttl if ttl is not None else current_ttl
            self.data[index] = (key, value, new_ttl)
        else:
            # Insert new key in sorted position
            new_item = (key, value, ttl)
            keys = [item[0] for item in self.data]
            insert_pos = bisect.bisect_left(keys, key)
            self.data.insert(insert_pos, new_item)
        
        return True
    
    def _delete_key(self, key: str) -> bool:
        """Internal method to delete a key"""
        index = self._find_key_index(key)
        if index != -1:
            self.data.pop(index)
            return True
        return False
    
    def _replay_log(self):
        """Replay the log file to rebuild state"""
        try:
            with open(self.log_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    parts = line.split()
                    if len(parts) < 2:
                        continue
                    
                    cmd = parts[0]
                    if cmd == "SET" and len(parts) >= 3:
                        key, value = parts[1], " ".join(parts[2:])
                        self._set_key(key, value, None)
                    elif cmd == "DEL" and len(parts) >= 2:
                        self._delete_key(parts[1])
                    elif cmd == "EXPIRE" and len(parts) >= 3:
                        key, ms = parts[1], parts[2]
                        index = self._find_key_index(key)
                        if index != -1:
                            ttl = time.time() * 1000 + float(ms)
                            key, value, _ = self.data[index]
                            self.data[index] = (key, value, ttl)
        except FileNotFoundError:
            pass  # First run, no log file
    
    def _write_to_log(self, command: str):
        """Write committed command to log file"""
        with open(self.log_file, 'a') as f:
            f.write(command + '\n')
    
    def _apply_transaction(self):
        """Apply all operations in transaction buffer to main store"""
        if not self.transaction_buffer:
            return
        
        for op, args in self.transaction_buffer:
            if op == "SET":
                key, value, ttl = args
                self._set_key(key, value, ttl)
                log_cmd = f"SET {key} {value}"
                self._write_to_log(log_cmd)
            elif op == "DEL":
                key = args[0]
                if self._delete_key(key):
                    self._write_to_log(f"DEL {key}")
            elif op == "EXPIRE":
                key, ms = args
                index = self._find_key_index(key)
                if index != -1:
                    ttl = time.time() * 1000 + float(ms)
                    key, value, _ = self.data[index]
                    self.data[index] = (key, value, ttl)
                    self._write_to_log(f"EXPIRE {key} {ms}")
    
    def set(self, key: str, value: str) -> str:
        if self.transaction_buffer is not None:
            # In transaction - buffer the operation
            current_ttl = None
            index = self._find_key_index(key)
            if index != -1:
                current_ttl = self.data[index][2]
            self.transaction_buffer.append(("SET", (key, value, current_ttl)))
        else:
            # Not in transaction - apply immediately
            self._set_key(key, value, None)
            self._write_to_log(f"SET {key} {value}")
        return "OK"
    
    def get(self, key: str) -> str:
        # Check transaction buffer first for read-your-writes
        if self.transaction_buffer is not None:
            # Look for latest operation on this key in transaction buffer
            for op, args in reversed(self.transaction_buffer):
                if op == "SET" and args[0] == key:
                    return args[1] if args[1] != "" else ""
                elif op == "DEL" and args[0] == key:
                    return "nil"
        
        index = self._get_key_index(key)
        if index == -1:
            return "nil"
        
        return self.data[index][1] if self.data[index][1] != "" else ""
    
    def delete(self, key: str) -> str:
        if self.transaction_buffer is not None:
            # In transaction - buffer the operation
            self.transaction_buffer.append(("DEL", (key,)))
            return "1"  # Assume it will be deleted
        else:
            # Not in transaction - apply immediately
            index = self._get_key_index(key, check_expired=False)
            if index != -1:
                self.data.pop(index)
                self._write_to_log(f"DEL {key}")
                return "1"
            return "0"
    
    def exists(self, key: str) -> str:
        if self.transaction_buffer is not None:
            # Check transaction buffer first
            for op, args in reversed(self.transaction_buffer):
                if op == "SET" and args[0] == key:
                    return "1"
                elif op == "DEL" and args[0] == key:
                    return "0"
        
        index = self._get_key_index(key)
        return "1" if index != -1 else "0"
    
    def mset(self, *args) -> str:
        if len(args) % 2 != 0:
            return "ERR wrong number of arguments for MSET"
        
        if self.transaction_buffer is not None:
            for i in range(0, len(args), 2):
                key, value = args[i], args[i+1]
                current_ttl = None
                index = self._find_key_index(key)
                if index != -1:
                    current_ttl = self.data[index][2]
                self.transaction_buffer.append(("SET", (key, value, current_ttl)))
        else:
            for i in range(0, len(args), 2):
                key, value = args[i], args[i+1]
                self._set_key(key, value, None)
                self._write_to_log(f"SET {key} {value}")
        
        return "OK"
    
    def mget(self, *keys) -> List[str]:
        results = []
        for key in keys:
            results.append(self.get(key))
        return results
    
    def begin(self) -> str:
        if self.transaction_buffer is not None:
            return "ERR transaction already in progress"
        self.transaction_buffer = []
        return "OK"
    
    def commit(self) -> str:
        if self.transaction_buffer is None:
            return "ERR no transaction in progress"
        
        self._apply_transaction()
        self.transaction_buffer = None
        return "OK"
    
    def abort(self) -> str:
        if self.transaction_buffer is None:
            return "ERR no transaction in progress"
        
        self.transaction_buffer = None
        return "OK"
    
    def expire(self, key: str, milliseconds: str) -> str:
        try:
            ms = float(milliseconds)
            if ms <= 0:
                # Expire immediately
                if self.transaction_buffer is not None:
                    self.transaction_buffer.append(("DEL", (key,)))
                else:
                    index = self._find_key_index(key)
                    if index != -1:
                        self.data.pop(index)
                        self._write_to_log(f"DEL {key}")
                return "1"
            
            ttl = time.time() * 1000 + ms
            
            if self.transaction_buffer is not None:
                # Check if key exists in main store or will be created in transaction
                exists = False
                for op, args in self.transaction_buffer:
                    if op == "SET" and args[0] == key:
                        exists = True
                        break
                if not exists:
                    index = self._find_key_index(key)
                    if index == -1 or self._is_expired(index):
                        return "0"
                self.transaction_buffer.append(("EXPIRE", (key, milliseconds)))
            else:
                index = self._get_key_index(key, check_expired=False)
                if index == -1:
                    return "0"
                key_name, value, _ = self.data[index]
                self.data[index] = (key_name, value, ttl)
                self._write_to_log(f"EXPIRE {key} {milliseconds}")
            
            return "1"
        except ValueError:
            return "ERR invalid TTL value"
    
    def ttl(self, key: str) -> str:
        # Check transaction buffer first
        if self.transaction_buffer is not None:
            for op, args in reversed(self.transaction_buffer):
                if op == "SET" and args[0] == key:
                    # SET without EXPIRE in same transaction means no TTL
                    return "-1"
                elif op == "DEL" and args[0] == key:
                    return "-2"
                elif op == "EXPIRE" and args[0] == key:
                    ms = float(args[1])
                    remaining = ms - (time.time() * 1000 - (time.time() * 1000 - ms))
                    return str(int(max(0, remaining)))
        
        index = self._get_key_index(key, check_expired=False)
        if index == -1:
            return "-2"
        
        if self._is_expired(index):
            return "-2"
        
        _, _, key_ttl = self.data[index]
        if key_ttl is None:
            return "-1"
        
        remaining = key_ttl - time.time() * 1000
        if remaining <= 0:
            self.data.pop(index)
            return "-2"
        
        return str(int(remaining))
    
    def persist(self, key: str) -> str:
        if self.transaction_buffer is not None:
            # Check if key exists and has TTL
            has_ttl = False
            for op, args in reversed(self.transaction_buffer):
                if op == "SET" and args[0] == key:
                    # If SET in transaction, check if it would have TTL
                    if args[2] is not None:
                        has_ttl = True
                    break
                elif op == "EXPIRE" and args[0] == key:
                    has_ttl = True
                    break
            
            if not has_ttl:
                index = self._find_key_index(key)
                if index == -1 or self._is_expired(index) or self.data[index][2] is None:
                    return "0"
            
            # Buffer a SET operation that preserves value but removes TTL
            current_value = None
            for op, args in reversed(self.transaction_buffer):
                if op == "SET" and args[0] == key:
                    current_value = args[1]
                    break
            
            if current_value is None:
                index = self._find_key_index(key)
                if index != -1 and not self._is_expired(index):
                    current_value = self.data[index][1]
            
            if current_value is not None:
                self.transaction_buffer.append(("SET", (key, current_value, None)))
                return "1"
            return "0"
        else:
            index = self._get_key_index(key)
            if index == -1:
                return "0"
            
            key_name, value, ttl = self.data[index]
            if ttl is None:
                return "0"
            
            self.data[index] = (key_name, value, None)
            # Note: PERSIST doesn't need to be logged as it's effectively a SET without TTL
            self._write_to_log(f"SET {key_name} {value}")
            return "1"
    
    def range(self, start: str, end: str) -> List[str]:
        result = []
        
        # Convert empty strings to None for open bounds
        start_key = start if start != "" else None
        end_key = end if end != "" else None
        
        for key, value, ttl in self.data:
            # Check bounds
            if start_key is not None and key < start_key:
                continue
            if end_key is not None and key > end_key:
                continue
            
            # Check if expired
            current_time = time.time() * 1000
            if ttl is not None and current_time > ttl:
                continue
            
            # Check transaction buffer for modifications
            if self.transaction_buffer is not None:
                key_in_txn = False
                for op, args in reversed(self.transaction_buffer):
                    if args[0] == key:
                        if op == "DEL":
                            key_in_txn = False
                            break
                        elif op == "SET":
                            key_in_txn = True
                            # Use the transaction value
                            break
                if not key_in_txn:
                    continue
            
            result.append(key)
        
        result.append("END")
        return result


def main():
    store = KVStore()
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        parts = line.split()
        if not parts:
            continue
        
        cmd = parts[0].upper()
        args = parts[1:]
        
        try:
            if cmd == "SET" and len(args) >= 2:
                key, value = args[0], " ".join(args[1:])
                print(store.set(key, value))
            elif cmd == "GET" and len(args) == 1:
                print(store.get(args[0]))
            elif cmd == "DEL" and len(args) == 1:
                print(store.delete(args[0]))
            elif cmd == "EXISTS" and len(args) == 1:
                print(store.exists(args[0]))
            elif cmd == "MSET" and len(args) >= 2:
                print(store.mset(*args))
            elif cmd == "MGET" and len(args) >= 1:
                results = store.mget(*args)
                for res in results:
                    print(res)
            elif cmd == "BEGIN" and len(args) == 0:
                print(store.begin())
            elif cmd == "COMMIT" and len(args) == 0:
                print(store.commit())
            elif cmd == "ABORT" and len(args) == 0:
                print(store.abort())
            elif cmd == "EXPIRE" and len(args) == 2:
                print(store.expire(args[0], args[1]))
            elif cmd == "TTL" and len(args) == 1:
                print(store.ttl(args[0]))
            elif cmd == "PERSIST" and len(args) == 1:
                print(store.persist(args[0]))
            elif cmd == "RANGE" and len(args) == 2:
                results = store.range(args[0], args[1])
                for res in results:
                    print(res)
            elif cmd == "EXIT":
                break
            else:
                print("ERR invalid command or arguments")
        except Exception as e:
            print(f"ERR {str(e)}")

if __name__ == "__main__":
    main()