// Copyright © 2024 Pathway

use std::io;

use cfg_if::cfg_if;

cfg_if! {
    if #[cfg(unix)] {
        use nix::fcntl::{fcntl, FcntlArg, FdFlag, OFlag};
        use nix::unistd;
        use std::os::fd::{AsFd, OwnedFd};
    } else if #[cfg(windows)] {
        use std::ptr::null_mut;
        use std::os::windows::io::{OwnedHandle, FromRawHandle};
        use windows_sys::Win32::System::Pipes::CreatePipe;
        use windows_sys::Win32::Foundation;
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum ReaderType {
    Blocking,
    NonBlocking,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum WriterType {
    Blocking,
    NonBlocking,
}

cfg_if! {
    if #[cfg(unix)] {
        #[derive(Debug)]
        pub struct Pipe {
            pub reader: OwnedFd,
            pub writer: OwnedFd,
        }
    } else if #[cfg(windows)] {
        #[derive(Debug)]
        pub struct Pipe {
            pub reader: OwnedHandle,
            pub writer: OwnedHandle,
        }
    }
}

#[cfg(unix)]
fn set_non_blocking(fd: impl AsFd) -> io::Result<()> {
    let fd = fd.as_fd();
    let flags = fcntl(fd, FcntlArg::F_GETFL)?;
    let flags = OFlag::from_bits_retain(flags);
    fcntl(fd, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK))?;
    Ok(())
}

#[cfg(unix)]
#[cfg_attr(target_os = "linux", allow(dead_code))]
fn set_cloexec(fd: impl AsFd) -> io::Result<()> {
    let fd = fd.as_fd();
    let flags = fcntl(fd, FcntlArg::F_GETFD)?;
    let flags = FdFlag::from_bits_retain(flags);
    fcntl(fd, FcntlArg::F_SETFD(flags | FdFlag::FD_CLOEXEC))?;
    Ok(())
}

pub fn pipe(reader_type: ReaderType, writer_type: WriterType) -> io::Result<Pipe> {
    cfg_if! {
        if #[cfg(unix)] {
            cfg_if! {
                if #[cfg(target_os = "linux")] {
                    let (reader, writer) = unistd::pipe2(OFlag::O_CLOEXEC)?;
                } else {
                    let (reader, writer) = unistd::pipe()?;
                    set_cloexec(&reader)?;
                    set_cloexec(&writer)?;
                }
            }

            if let ReaderType::NonBlocking = reader_type {
                set_non_blocking(&reader)?;
            }

            if let WriterType::NonBlocking = writer_type {
                set_non_blocking(&writer)?;
            }

            Ok(Pipe { reader, writer })
        } else if #[cfg(windows)] {
            use Foundation::{HANDLE, INVALID_HANDLE_VALUE, CloseHandle};
            
            // For Windows, we'll use anonymous pipes
            // with named pipes and overlapped I/O operations
            let mut read_handle: HANDLE = null_mut();
            let mut write_handle: HANDLE = null_mut();
            
            let success = unsafe {
                CreatePipe(
                    &mut read_handle,
                    &mut write_handle,
                    null_mut(), // Default security attributes
                    65536,      // Buffer size 64KB
                )
            };
            
            if success == 0 {
                return Err(io::Error::last_os_error());
            }

            if read_handle == INVALID_HANDLE_VALUE || read_handle.is_null() ||
               write_handle == INVALID_HANDLE_VALUE || write_handle.is_null() {

                if !read_handle.is_null() && read_handle != INVALID_HANDLE_VALUE {
                    unsafe { CloseHandle(read_handle); }
                }
                if !write_handle.is_null() && write_handle != INVALID_HANDLE_VALUE {
                    unsafe { CloseHandle(write_handle); }
                }
                return Err(io::Error::new(
                    io::ErrorKind::Other, 
                    "Failed to create valid pipe handles"
                ));
            }

            // Wrap raw handles in OwnedHandle for automatic resource management
            let reader = unsafe { OwnedHandle::from_raw_handle(read_handle) };
            let writer = unsafe { OwnedHandle::from_raw_handle(write_handle) };
            
            Ok(Pipe { reader, writer })
        }
    }
}
