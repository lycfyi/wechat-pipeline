import sys
import os
import glob
import lldb
import time
import json

# WeChat db_storage directory — replace with your own path!
DB_DIR = os.path.expanduser(
    "~/Library/Containers/com.tencent.xinWeChat/Data/Documents/xwechat_files"
)
PAGE_SZ = 4096
SALT_SZ = 16
OUTPUT_FILE = "wechat_keys.json"


def find_db_dir():
    """Auto-detect the db_storage directory under DB_DIR."""
    pattern = os.path.join(DB_DIR, "*", "db_storage")
    candidates = glob.glob(pattern)
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        print(f"[*] Found multiple db_storage dirs, using first: {candidates[0]}")
        return candidates[0]
    # Fallback: maybe user put DB_DIR directly pointing to db_storage
    if os.path.isdir(DB_DIR) and os.path.basename(DB_DIR) == "db_storage":
        return DB_DIR
    return None


def build_salt_to_db_map(db_dir):
    """Read the first page of each .db file to extract the salt, return salt_hex -> [rel_paths]."""
    salt_to_dbs = {}  # salt_hex -> [rel_path, ...]
    for root, dirs, files in os.walk(db_dir):
        for f in files:
            if not f.endswith(".db"):
                continue
            if f.endswith("-wal") or f.endswith("-shm"):
                continue
            path = os.path.join(root, f)
            rel = os.path.relpath(path, db_dir)
            sz = os.path.getsize(path)
            if sz < PAGE_SZ:
                continue
            with open(path, "rb") as fh:
                page1 = fh.read(PAGE_SZ)
            salt = page1[:SALT_SZ].hex()
            salt_to_dbs.setdefault(salt, []).append(rel)
    return salt_to_dbs


def find_wechat_key():
    # Initialize debugger
    debugger = lldb.SBDebugger.Create()
    debugger.SetAsync(False)

    target = debugger.CreateTarget("")
    error = lldb.SBError()

    # Attach to WeChat
    print("[*] Attaching to WeChat...")
    process = target.AttachToProcessWithName(
        debugger.GetListener(), "WeChat", False, error
    )
    if not error.Success():
        print(f"[-] Error attaching to WeChat: {error.GetCString()}")
        print(
            "[!] Make sure WeChat is running and you have necessary permissions (or SIP is disabled)."
        )
        return

    print(f"[+] Attached to WeChat (PID: {process.GetProcessID()})")

    target = debugger.GetSelectedTarget()

    # Find the WeChat module
    wechat_module = None
    for m in target.module_iter():
        if m.GetFileSpec().GetFilename() == "WeChat":
            wechat_module = m
            break

    if not wechat_module:
        print("[-] WeChat module not found.")
        process.Detach()
        return

    # Strategy: statically find the setCipherKey function by searching for
    # "mov w0, #0x43" (malloc(67)) + "bl malloc" pattern in WeChat's __text.

    # Find __TEXT,__text section
    text_addr = 0
    text_size = 0
    for i in range(wechat_module.GetNumSections()):
        sec = wechat_module.GetSectionAtIndex(i)
        if sec.GetName() == "__TEXT":
            for j in range(sec.GetNumSubSections()):
                subsec = sec.GetSubSectionAtIndex(j)
                if subsec.GetName() == "__text":
                    text_addr = subsec.GetLoadAddress(target)
                    text_size = subsec.GetByteSize()
                    break
            break

    if not text_addr:
        print("[-] Could not find __TEXT,__text section.")
        process.Detach()
        return

    print(f"[*] WeChat __TEXT,__text: {hex(text_addr)} - {hex(text_addr + text_size)}")

    setCipherKey_addr = None
    function_name = None

    # Resolve malloc address
    malloc_syms = target.FindSymbols("malloc")
    malloc_addr = None
    for sym_ctx in malloc_syms:
        sym = sym_ctx.GetSymbol()
        if sym.IsValid():
            malloc_addr = sym.GetStartAddress().GetLoadAddress(target)
            break

    if not malloc_addr:
        print("[-] Could not resolve malloc address.")
        process.Detach()
        return
    print(f"[*] malloc at {hex(malloc_addr)}")

    # Find all mov w0/x0, #0x43 instructions
    # -e takes an expression (integer), not hex byte strings.
    # mov w0, #0x43 = 0x52800860, mov x0, #0x43 = 0xd2800860
    candidates = []
    for pattern_name, pattern_int in [
        ("mov w0, #0x43", 0x52800860),
        ("mov x0, #0x43", 0xD2800860),
    ]:
        search_start = text_addr
        search_end = text_addr + text_size
        while search_start < search_end:
            res = lldb.SBCommandReturnObject()
            find_cmd = (
                f"memory find -e (uint32_t){hex(pattern_int)} -- "
                f"{hex(search_start)} {hex(search_end)}"
            )
            debugger.GetCommandInterpreter().HandleCommand(find_cmd, res)
            if not res.Succeeded() or "data found" not in res.GetOutput():
                break
            found = False
            for line in res.GetOutput().strip().split("\n"):
                if "0x" in line and "data found" not in line:
                    addr_str = line.strip().split("0x")[-1].split()[0]
                    addr_str = addr_str.rstrip(":")  # remove trailing colon
                    addr = int(addr_str, 16)
                    candidates.append((addr, pattern_name))
                    search_start = addr + 4
                    found = True
                    break
            if not found:
                break

    print(f"[*] Found {len(candidates)} mov x0/w0, #0x43 instructions")

    # Check each: must have bl <something that resolves to malloc> nearby.
    # On macOS ARM64, bl targets a stub in __stubs, not malloc directly,
    # so we resolve the symbol at the bl target address.
    for addr, pname in candidates:
        has_bl_malloc = False
        for offset in range(4, 20, 4):
            instr_addr = addr + offset
            instr_bytes = process.ReadMemory(instr_addr, 4, error)
            if not error.Success():
                continue
            instr = int.from_bytes(instr_bytes, "little")
            if (instr >> 26) == 0b100101:  # BL
                imm26 = instr & 0x03FFFFFF
                if imm26 & 0x02000000:
                    imm26 |= ~0x03FFFFFF
                    imm26 &= 0xFFFFFFFFFFFFFFFF
                bl_target = (instr_addr + (imm26 << 2)) & 0xFFFFFFFFFFFFFFFF
                # Check if bl target resolves to malloc (direct or via stub)
                if bl_target == malloc_addr:
                    has_bl_malloc = True
                    break
                # Resolve symbol at bl target (could be __stubs entry)
                bl_sym = target.ResolveLoadAddress(bl_target).GetSymbol()
                if bl_sym.IsValid() and "malloc" == bl_sym.GetName():
                    has_bl_malloc = True
                    break

        if not has_bl_malloc:
            continue

        print(f"[+] Found {pname} at {hex(addr)} + bl malloc")

        sb_addr = target.ResolveLoadAddress(addr)
        sym = sb_addr.GetSymbol()
        if sym.IsValid():
            func_start = sym.GetStartAddress().GetLoadAddress(target)
            fname = sym.GetName()
            print(f"[+] -> In function {fname} at {hex(func_start)}")
            setCipherKey_addr = func_start
            function_name = fname
        else:
            setCipherKey_addr = addr
            function_name = f"unknown@{hex(addr)}"

        break  # use first match

    if setCipherKey_addr is None:
        print("[-] Could not find setCipherKey function.")
        process.Detach()
        return

    print(f"[+] setCipherKey function: {hex(setCipherKey_addr)} ({function_name})")

    # Set breakpoint on the function
    bp = target.BreakpointCreateByAddress(setCipherKey_addr)
    print(f"[+] Set breakpoint at {hex(setCipherKey_addr)}")

    # Verify exactly one breakpoint
    num_bps = target.GetNumBreakpoints()
    if num_bps != 1:
        print(
            f"[-] Expected 1 breakpoint but found {num_bps}. Removing all and aborting."
        )
        debugger.GetCommandInterpreter().HandleCommand(
            "break delete -f", lldb.SBCommandReturnObject()
        )
        process.Detach()
        return

    def wait_for_stop(process, listener):
        """Wait for process to stop using event listener. Allows KeyboardInterrupt."""
        event = lldb.SBEvent()
        while True:
            # 1-second timeout allows Python to handle Ctrl+C between iterations
            if listener.WaitForEvent(1, event):
                state = lldb.SBProcess.GetStateFromEvent(event)
                if state == lldb.eStateStopped:
                    return True
                if state in (
                    lldb.eStateExited,
                    lldb.eStateCrashed,
                    lldb.eStateDetached,
                ):
                    print(f"[-] Process ended unexpectedly (state={state}).")
                    return False
                # eStateRunning or other transient states - keep waiting
        return False

    listener = debugger.GetListener()

    try:
        # Switch to async mode so process.Continue() returns immediately,
        # allowing Python to handle KeyboardInterrupt (Ctrl+C).
        debugger.SetAsync(True)

        print("[*] Continuing to collect keys. Press Ctrl+C to stop.")

        # Build salt -> db file mapping
        db_dir = find_db_dir()
        salt_to_dbs = {}
        if db_dir:
            print(f"[*] Scanning db files in: {db_dir}")
            salt_to_dbs = build_salt_to_db_map(db_dir)
            print(f"[*] Found {len(salt_to_dbs)} unique salts across db files")
        else:
            print(f"[!] Could not find db_storage directory under {DB_DIR}")
            print("[!] Output will use salt as key identifier instead of db path")

        # db_path -> key (or salt -> key if no db mapping)
        result = {}
        seen_salts = set()
        # Load existing results if the file exists
        if os.path.exists(OUTPUT_FILE):
            try:
                with open(OUTPUT_FILE, "r") as f:
                    result = json.load(f)
                # Rebuild seen_salts from existing results
                seen_salts = set(result.get("__salts__", []))
                print(
                    f"[*] Loaded {len(result) - (1 if '__salts__' in result else 0)} existing entries from {OUTPUT_FILE}"
                )
            except Exception:
                pass

        def save_keys():
            # Store seen_salts for dedup across runs
            output = dict(result)
            output["__salts__"] = sorted(seen_salts)
            with open(OUTPUT_FILE, "w") as f:
                json.dump(output, f, indent=2, ensure_ascii=False)
            count = len(output) - 1  # exclude __salts__
            print(f"[*] Saved {count} keys to {OUTPUT_FILE}")

        def parse_and_store_key(raw_key_str):
            """Parse x'<64-char key><32-char salt>', map to db files, store.
            Returns True if a new key was added."""
            inner = raw_key_str[2:-1]  # remove x' and '
            if len(inner) != 96:
                print(f"[!] Unexpected key length {len(inner)}: {raw_key_str}")
                return False
            key = inner[:64]
            salt = inner[64:]
            if salt in seen_salts:
                return False
            seen_salts.add(salt)
            db_paths = salt_to_dbs.get(salt, [])
            if not db_paths and db_dir:
                # Salt not found — rescan db files in case new databases were created
                print(f"[*] Unknown salt {salt}, rescanning db files...")
                salt_to_dbs.update(build_salt_to_db_map(db_dir))
                db_paths = salt_to_dbs.get(salt, [])
            if db_paths:
                for db_path in db_paths:
                    result[db_path] = key
                print(f"\n[!] Found new key!  salt={salt}  key={key}")
                print(f"    Matched db files: {db_paths}")
            else:
                # No db file matched — store with salt as identifier
                result[f"unknown_salt_{salt}"] = key
                print(f"\n[!] Found new key!  salt={salt}  key={key}")
                print(f"    No matching db file found for this salt")
            save_keys()
            return True

        while True:
            process.Continue()

            if not wait_for_stop(process, listener):
                break

            # Find the thread that hit the breakpoint
            thread = None
            for i in range(process.GetNumThreads()):
                t = process.GetThreadAtIndex(i)
                if t.GetStopReason() == lldb.eStopReasonBreakpoint:
                    thread = t
                    break

            if thread is None:
                # No thread hit breakpoint — transient stop (signal, etc.), re-continue
                continue

            frame = thread.GetFrameAtIndex(0)
            # ARM64: x1 is the pointer to UnsafeData
            x1 = frame.FindRegister("x1").GetValueAsUnsigned()

            # Read pointer to string at x1 + 8
            ptr_addr = x1 + 8
            ptr = process.ReadPointerFromMemory(ptr_addr, error)
            if error.Success() and ptr != 0:
                # Read string at ptr
                # Key string is like x'0123...DEF' (98 characters + null?)
                data = process.ReadCStringFromMemory(ptr, 128, error)
                if error.Success():
                    try:
                        # Find end of string
                        end_idx = data.find("'", 2)  # find the closing quote
                        if end_idx != -1:
                            key_str = data[: end_idx + 1]
                            if key_str.startswith("x'"):
                                parse_and_store_key(key_str)
                    except Exception as e:
                        print(f"Error parsing key: {e}")
    except KeyboardInterrupt:
        print("\n[*] Stopped by user.")
    finally:
        if seen_salts:
            save_keys()
        process.Detach()
        print("[*] Detached from WeChat.")


if __name__ == "__main__":
    find_wechat_key()
