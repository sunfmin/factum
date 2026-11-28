//! Owner-only files (ADR 0021): mode 0600 on Unix; on Windows a protected
//! access list (nothing inherited) granting full control only to the file's
//! owner, SYSTEM and the Administrators group. Set on the open handle every
//! time a file is opened, so a new file is owner-only before any byte is in
//! it and one left wider is tightened.

use std::fs::{File, OpenOptions};
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;
use std::path::Path;

/// Opens `path` with `options`, creating it if missing, owner-only whatever
/// access it was left with.
pub(crate) fn owner_only(path: &Path, options: &mut OpenOptions) -> std::io::Result<File> {
    #[cfg(unix)]
    options.mode(0o600);
    #[cfg(windows)]
    options.access_mode(windows::ACCESS);
    let file = options.create(true).open(path)?;
    #[cfg(unix)]
    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    #[cfg(windows)]
    windows::restrict(&file).map_err(std::io::Error::other)?;
    Ok(file)
}

#[cfg(windows)]
#[allow(unsafe_code)]
mod windows {
    use std::fs::File;
    use std::os::windows::io::AsRawHandle;
    use std::ptr::null_mut;

    use windows_sys::Win32::Foundation::{ERROR_SUCCESS, GENERIC_READ, GENERIC_WRITE, LocalFree};
    use windows_sys::Win32::Security::Authorization::{
        ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
        GetSecurityInfo, SDDL_REVISION_1, SE_FILE_OBJECT, SetSecurityInfo,
    };
    use windows_sys::Win32::Security::{
        DACL_SECURITY_INFORMATION, GetSecurityDescriptorDacl, OWNER_SECURITY_INFORMATION,
        PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID,
    };
    use windows_sys::Win32::Storage::FileSystem::WRITE_DAC;

    /// Read, write, and the right to replace the access list.
    pub const ACCESS: u32 = GENERIC_READ | GENERIC_WRITE | WRITE_DAC;

    /// Memory Windows allocated, freed on drop.
    struct Local(*mut core::ffi::c_void);

    impl Drop for Local {
        fn drop(&mut self) {
            // SAFETY: allocated by Windows with LocalAlloc, freed once.
            unsafe { LocalFree(self.0) };
        }
    }

    /// Replaces `file`'s access list by a protected one granting full
    /// control to its owner, SYSTEM and the Administrators group.
    pub fn restrict(file: &File) -> Result<(), String> {
        let (mut owner, mut sd): (PSID, PSECURITY_DESCRIPTOR) = (null_mut(), null_mut());
        // SAFETY: a valid open handle; the out pointers are written by the call.
        let status = unsafe {
            GetSecurityInfo(
                file.as_raw_handle(),
                SE_FILE_OBJECT,
                OWNER_SECURITY_INFORMATION,
                &mut owner,
                null_mut(),
                null_mut(),
                null_mut(),
                &mut sd,
            )
        };
        if status != ERROR_SUCCESS {
            return Err(format!("GetSecurityInfo failed with error {status}"));
        }
        // `owner` points into `sd`, alive until `_sd` drops.
        let _sd = Local(sd);
        let sddl = format!(
            "D:P(A;;FA;;;{})(A;;FA;;;SY)(A;;FA;;;BA)",
            sid_string(owner)?
        );
        let wide: Vec<u16> = sddl.encode_utf16().chain([0]).collect();
        let mut built: PSECURITY_DESCRIPTOR = null_mut();
        // SAFETY: a NUL-terminated wide string; the call allocates `built`.
        let parsed = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                wide.as_ptr(),
                SDDL_REVISION_1,
                &mut built,
                null_mut(),
            )
        };
        if parsed == 0 {
            return Err(format!("the access list `{sddl}` does not parse"));
        }
        let _built = Local(built);
        let (mut present, mut defaulted, mut dacl) = (0, 0, null_mut());
        // SAFETY: `built` is a valid descriptor; the out pointers are written by the call.
        if unsafe { GetSecurityDescriptorDacl(built, &mut present, &mut dacl, &mut defaulted) } == 0
        {
            return Err("GetSecurityDescriptorDacl failed".to_string());
        }
        // SAFETY: a valid handle opened with WRITE_DAC, and the list inside `built`.
        let status = unsafe {
            SetSecurityInfo(
                file.as_raw_handle(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                dacl,
                null_mut(),
            )
        };
        if status != ERROR_SUCCESS {
            return Err(format!("SetSecurityInfo failed with error {status}"));
        }
        Ok(())
    }

    fn sid_string(sid: PSID) -> Result<String, String> {
        let mut text = null_mut();
        // SAFETY: `sid` is valid; the call allocates `text`.
        if unsafe { ConvertSidToStringSidW(sid, &mut text) } == 0 {
            return Err("the file's owner has no SID string".to_string());
        }
        let _text = Local(text.cast());
        // SAFETY: a NUL-terminated wide string from the call.
        let len = (0..).take_while(|&i| unsafe { *text.add(i) } != 0).count();
        // SAFETY: `len` wide chars are there.
        Ok(String::from_utf16_lossy(unsafe {
            std::slice::from_raw_parts(text, len)
        }))
    }
}
