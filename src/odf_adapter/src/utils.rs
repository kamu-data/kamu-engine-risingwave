use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use internal_error::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[tracing::instrument(level = "debug")]
pub async fn wait_for_socket(addr: &SocketAddr, timeout: Duration) -> Result<(), InternalError> {
    let start = Instant::now();

    loop {
        if check_socket(addr)? {
            break Ok(());
        } else if start.elapsed() >= timeout {
            break Err(format!("Timeout after {} seconds", timeout.as_secs_f32()).int_err());
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

fn check_socket(addr: &SocketAddr) -> Result<bool, InternalError> {
    use std::io::Read;

    let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(100)) else {
        return Ok(false);
    };

    stream
        .set_read_timeout(Some(Duration::from_millis(1000)))
        .int_err()?;

    let mut buf = [0; 1];
    match stream.read(&mut buf) {
        Ok(0) => Ok(false),
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::TimedOut => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => Ok(false),
        Err(e) => Err(e.int_err()),
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
