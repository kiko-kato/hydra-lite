use regex::Regex;
use std::sync::LazyLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockDiagnostic {
    pub port: u16,
    pub pid: u32,
    pub process_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileLockDiagnostic {
    pub filename: String,
    pub pid: u32,
    pub process_name: String,
}

static PORT_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:(?::(\d{2,5}))|(?:\bport\b\s*[:=]?\s*(\d{2,5})))").expect("Valid regex")
});

static EXE_LOCK_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:os error [0-9]+|access is denied|sharing violation|cannot open file).*?([a-zA-Z0-9_\-]+\.exe)|([a-zA-Z0-9_\-]+\.exe).*?(?:os error [0-9]+|access is denied)")
        .expect("Valid regex")
});

#[cfg(windows)]
mod winapi_impl {
    use std::ffi::c_void;
    use std::mem::size_of;

    const AF_INET: u32 = 2;
    const TCP_TABLE_OWNER_PID_ALL: i32 = 5;
    const TH32CS_SNAPPROCESS: u32 = 0x00000002;

    #[repr(C)]
    struct MIB_TCPROW_OWNER_PID {
        dw_state: u32,
        dw_local_addr: u32,
        dw_local_port: u32,
        dw_remote_addr: u32,
        dw_remote_port: u32,
        dw_owning_pid: u32,
    }

    #[repr(C)]
    struct MIB_TCPTABLE_OWNER_PID {
        dw_num_entries: u32,
        table: [MIB_TCPROW_OWNER_PID; 1],
    }

    #[repr(C)]
    struct PROCESSENTRY32W {
        dw_size: u32,
        cnt_usage: u32,
        th32_process_id: u32,
        th32_default_heap_id: usize,
        th32_module_id: u32,
        cnt_threads: u32,
        th32_parent_process_id: u32,
        pc_pri_class_base: i32,
        dw_flags: u32,
        sz_exe_file: [u16; 260],
    }

    #[link(name = "iphlpapi")]
    extern "system" {
        fn GetExtendedTcpTable(
            pTcpTable: *mut c_void,
            pdwSize: *mut u32,
            bOrder: i32,
            ulAf: u32,
            TableClass: i32,
            Reserved: u32,
        ) -> u32;
    }

    extern "system" {
        fn CreateToolhelp32Snapshot(dwFlags: u32, th32ProcessID: u32) -> *mut c_void;
        fn Process32FirstW(hSnapshot: *mut c_void, lppe: *mut PROCESSENTRY32W) -> i32;
        fn Process32NextW(hSnapshot: *mut c_void, lppe: *mut PROCESSENTRY32W) -> i32;
        fn CloseHandle(hObject: *mut c_void) -> i32;
    }

    pub fn get_process_name(pid: u32) -> Option<String> {
        unsafe {
            let snap = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);
            if snap.is_null() || snap as isize == -1 {
                return None;
            }

            let mut entry: PROCESSENTRY32W = std::mem::zeroed();
            entry.dw_size = size_of::<PROCESSENTRY32W>() as u32;

            let mut found = None;
            if Process32FirstW(snap, &mut entry) != 0 {
                loop {
                    if entry.th32_process_id == pid {
                        let len = entry
                            .sz_exe_file
                            .iter()
                            .position(|&c| c == 0)
                            .unwrap_or(entry.sz_exe_file.len());
                        found = Some(String::from_utf16_lossy(&entry.sz_exe_file[..len]));
                        break;
                    }
                    if Process32NextW(snap, &mut entry) == 0 {
                        break;
                    }
                }
            }

            CloseHandle(snap);
            found
        }
    }

    pub fn find_pid_by_name(exe_name: &str) -> Option<u32> {
        let target = exe_name.to_lowercase();
        unsafe {
            let snap = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);
            if snap.is_null() || snap as isize == -1 {
                return None;
            }

            let mut entry: PROCESSENTRY32W = std::mem::zeroed();
            entry.dw_size = size_of::<PROCESSENTRY32W>() as u32;

            let mut found_pid = None;
            if Process32FirstW(snap, &mut entry) != 0 {
                loop {
                    let len = entry
                        .sz_exe_file
                        .iter()
                        .position(|&c| c == 0)
                        .unwrap_or(entry.sz_exe_file.len());
                    let name = String::from_utf16_lossy(&entry.sz_exe_file[..len]).to_lowercase();
                    if name == target {
                        found_pid = Some(entry.th32_process_id);
                        break;
                    }
                    if Process32NextW(snap, &mut entry) == 0 {
                        break;
                    }
                }
            }

            CloseHandle(snap);
            found_pid
        }
    }

    pub fn find_pid_by_port(target_port: u16) -> Option<u32> {
        unsafe {
            let mut size: u32 = 0;
            let _ = GetExtendedTcpTable(
                std::ptr::null_mut(),
                &mut size,
                0,
                AF_INET,
                TCP_TABLE_OWNER_PID_ALL,
                0,
            );

            if size == 0 {
                return None;
            }

            let mut buffer = vec![0u8; size as usize];
            let ret = GetExtendedTcpTable(
                buffer.as_mut_ptr() as *mut c_void,
                &mut size,
                0,
                AF_INET,
                TCP_TABLE_OWNER_PID_ALL,
                0,
            );

            if ret != 0 {
                return None;
            }

            let table = &*(buffer.as_ptr() as *const MIB_TCPTABLE_OWNER_PID);
            let entries = std::slice::from_raw_parts(
                &table.table[0] as *const MIB_TCPROW_OWNER_PID,
                table.dw_num_entries as usize,
            );

            for row in entries {
                let port = u16::from_be((row.dw_local_port & 0xFFFF) as u16);
                if port == target_port && row.dw_owning_pid > 0 {
                    return Some(row.dw_owning_pid);
                }
            }

            None
        }
    }
}

#[cfg(not(windows))]
mod winapi_impl {
    pub fn get_process_name(_pid: u32) -> Option<String> {
        None
    }
    pub fn find_pid_by_name(_exe_name: &str) -> Option<u32> {
        None
    }
    pub fn find_pid_by_port(_target_port: u16) -> Option<u32> {
        None
    }
}

pub fn resolve_port_lock(text: &str) -> Option<LockDiagnostic> {
    let lower = text.to_lowercase();
    if !lower.contains("10048")
        && !lower.contains("address already in use")
        && !lower.contains("wsaeaddrinuse")
    {
        return None;
    }

    let caps = PORT_REGEX.captures(text)?;
    let port_str = caps
        .get(1)
        .or_else(|| caps.get(2))
        .or_else(|| caps.get(3))?
        .as_str();
    let port: u16 = port_str.parse().ok()?;

    let pid = winapi_impl::find_pid_by_port(port).unwrap_or(0);
    let process_name = if pid > 0 {
        winapi_impl::get_process_name(pid).unwrap_or_else(|| "unknown".to_string())
    } else {
        "unknown".to_string()
    };

    Some(LockDiagnostic {
        port,
        pid,
        process_name,
    })
}

pub fn resolve_file_lock(text: &str) -> Option<FileLockDiagnostic> {
    let lower = text.to_lowercase();
    if !lower.contains("os error 5")
        && !lower.contains("access is denied")
        && !lower.contains("error_sharing_violation")
    {
        return None;
    }

    let caps = EXE_LOCK_REGEX.captures(text)?;
    let filename = caps.get(1).or_else(|| caps.get(2))?.as_str().to_string();

    let pid = winapi_impl::find_pid_by_name(&filename).unwrap_or(0);
    let process_name = if pid > 0 {
        winapi_impl::get_process_name(pid).unwrap_or_else(|| filename.clone())
    } else {
        filename.clone()
    };

    Some(FileLockDiagnostic {
        filename,
        pid,
        process_name,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_port() {
        let msg = "failed to bind to 127.0.0.1:18080: os error 10048 (Only one usage of each socket address is normally permitted)";
        let diag = resolve_port_lock(msg).expect("Must find lock");
        assert_eq!(diag.port, 18080);
    }

    #[test]
    fn test_file_lock() {
        let msg = "error: could not compile `test` Access is denied (os error 5) target/release/hydra-headless.exe";
        let diag = resolve_file_lock(msg).expect("Must find file lock");
        assert_eq!(diag.filename, "hydra-headless.exe");
    }
}
