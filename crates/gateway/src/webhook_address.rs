//! Passive interface discovery for links issued by this Gateway.
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use anyhow::Result;
use pioneer_protocol::GatewayBaseUrl;

pub(crate) fn local_webhook_base_url(bind: SocketAddr) -> Result<(GatewayBaseUrl, &'static str)> {
    if let SocketAddr::V6(address) = bind {
        // GatewayBaseUrl/Url do not support zone identifiers. Check before
        // extracting IpAddr, which would silently discard the listener's zone.
        anyhow::ensure!(
            address.scope_id() == 0 && !address.ip().is_unicast_link_local(),
            "webhook URLs for scoped or link-local IPv6 listeners are unsupported: GatewayBaseUrl cannot preserve the required zone identifier"
        );
    }
    let destination = if bind.ip().is_unspecified() {
        choose_destination(bind.ip(), interface_addresses().unwrap_or_default())
    } else {
        bind.ip()
    };
    let scope = if destination.is_loopback() {
        "loopback"
    } else {
        "lan"
    };
    let base = GatewayBaseUrl::from_local_listen_addr(
        &SocketAddr::new(destination, bind.port()).to_string(),
    )?;
    Ok((base, scope))
}

fn choose_destination(bind: IpAddr, mut candidates: Vec<IpAddr>) -> IpAddr {
    candidates.retain(|ip| {
        ip.is_ipv4() == bind.is_ipv4()
            && !ip.is_unspecified()
            && !ip.is_loopback()
            && !ip.is_multicast()
            // Scoped link-local IPv6 addresses cannot be used as a portable URL.
            && !matches!(ip, IpAddr::V6(ip) if ip.is_unicast_link_local())
    });
    candidates.sort_by_key(|ip| {
        let priority = match ip {
            IpAddr::V4(ip) if ip.is_private() => 0,
            IpAddr::V6(ip) if ip.is_unique_local() => 0,
            IpAddr::V4(ip) if ip.is_link_local() => 2,
            _ => 1,
        };
        (priority, *ip)
    });
    candidates.into_iter().next().unwrap_or_else(|| match bind {
        IpAddr::V4(_) => Ipv4Addr::LOCALHOST.into(),
        IpAddr::V6(_) => Ipv6Addr::LOCALHOST.into(),
    })
}

#[cfg(unix)]
fn interface_addresses() -> Result<Vec<IpAddr>> {
    let mut head = std::ptr::null_mut();
    // getifaddrs reads the kernel's interface list. No sockets or probes are used.
    if unsafe { libc::getifaddrs(&mut head) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    struct IfAddrs(*mut libc::ifaddrs);
    impl Drop for IfAddrs {
        fn drop(&mut self) {
            unsafe { libc::freeifaddrs(self.0) };
        }
    }
    let interfaces = IfAddrs(head);
    let mut current = interfaces.0;
    let mut addresses = Vec::new();
    while !current.is_null() {
        // Every address belongs to the list retained by `interfaces`.
        let interface = unsafe { &*current };
        let flags = interface.ifa_flags as libc::c_int;
        if !interface.ifa_addr.is_null()
            && flags & libc::IFF_UP != 0
            && flags & libc::IFF_RUNNING != 0
            && flags & libc::IFF_LOOPBACK == 0
        {
            match unsafe { (*interface.ifa_addr).sa_family as libc::c_int } {
                libc::AF_INET => {
                    let socket = unsafe { &*interface.ifa_addr.cast::<libc::sockaddr_in>() };
                    addresses.push(Ipv4Addr::from(socket.sin_addr.s_addr.to_ne_bytes()).into());
                }
                libc::AF_INET6 => {
                    let socket = unsafe { &*interface.ifa_addr.cast::<libc::sockaddr_in6>() };
                    addresses.push(Ipv6Addr::from(socket.sin6_addr.s6_addr).into());
                }
                _ => {}
            }
        }
        current = interface.ifa_next;
    }
    Ok(addresses)
}

#[cfg(windows)]
fn interface_addresses() -> Result<Vec<IpAddr>> {
    use windows_sys::Win32::Foundation::{ERROR_BUFFER_OVERFLOW, NO_ERROR};
    use windows_sys::Win32::NetworkManagement::IpHelper::{
        GAA_FLAG_SKIP_ANYCAST, GAA_FLAG_SKIP_DNS_SERVER, GAA_FLAG_SKIP_MULTICAST,
        GetAdaptersAddresses, IF_TYPE_SOFTWARE_LOOPBACK, IP_ADAPTER_ADDRESSES_LH,
    };
    use windows_sys::Win32::NetworkManagement::Ndis::IfOperStatusUp;
    use windows_sys::Win32::Networking::WinSock::{
        AF_INET, AF_INET6, AF_UNSPEC, IpDadStatePreferred, SOCKADDR_IN, SOCKADDR_IN6,
    };
    let flags = GAA_FLAG_SKIP_ANYCAST | GAA_FLAG_SKIP_DNS_SERVER | GAA_FLAG_SKIP_MULTICAST;
    let mut size = 15_000u32;
    // The interface list can change between reads. Bound both retries and memory.
    for _ in 0..3 {
        anyhow::ensure!(size <= 1_048_576, "interface address list is too large");
        // Typed allocation supplies the alignment required by the linked structs.
        let count = (size as usize).div_ceil(std::mem::size_of::<IP_ADAPTER_ADDRESSES_LH>());
        let mut buffer = vec![IP_ADAPTER_ADDRESSES_LH::default(); count];
        let result = unsafe {
            GetAdaptersAddresses(
                AF_UNSPEC as u32,
                flags,
                std::ptr::null(),
                buffer.as_mut_ptr(),
                &mut size,
            )
        };
        if result == ERROR_BUFFER_OVERFLOW {
            continue;
        }
        if result != NO_ERROR {
            return Err(std::io::Error::from_raw_os_error(result as i32).into());
        }
        let mut addresses = Vec::new();
        let mut current = buffer.as_ptr();
        while !current.is_null() {
            // The successful system call initialized this linked list in buffer.
            let adapter = unsafe { &*current };
            if adapter.OperStatus == IfOperStatusUp && adapter.IfType != IF_TYPE_SOFTWARE_LOOPBACK {
                let mut unicast = adapter.FirstUnicastAddress;
                while !unicast.is_null() {
                    let address = unsafe { &*unicast };
                    let socket = address.Address.lpSockaddr;
                    if !socket.is_null() && address.DadState == IpDadStatePreferred {
                        match unsafe { (*socket).sa_family } {
                            AF_INET
                                if address.Address.iSockaddrLength as usize
                                    >= std::mem::size_of::<SOCKADDR_IN>() =>
                            {
                                let socket = unsafe { &*socket.cast::<SOCKADDR_IN>() };
                                addresses.push(
                                    Ipv4Addr::from(
                                        unsafe { socket.sin_addr.S_un.S_addr }.to_ne_bytes(),
                                    )
                                    .into(),
                                );
                            }
                            AF_INET6
                                if address.Address.iSockaddrLength as usize
                                    >= std::mem::size_of::<SOCKADDR_IN6>() =>
                            {
                                let socket = unsafe { &*socket.cast::<SOCKADDR_IN6>() };
                                addresses.push(
                                    Ipv6Addr::from(unsafe { socket.sin6_addr.u.Byte }).into(),
                                );
                            }
                            _ => {}
                        }
                    }
                    unicast = address.Next;
                }
            }
            current = adapter.Next;
        }
        return Ok(addresses);
    }
    anyhow::bail!("interface address list changed repeatedly")
}

#[cfg(not(any(unix, windows)))]
fn interface_addresses() -> Result<Vec<IpAddr>> {
    Ok(Vec::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildcard_selection_respects_listener_family_and_ignores_unusable_addresses() {
        let addresses = vec![
            "127.0.0.1".parse().unwrap(),
            "0.0.0.0".parse().unwrap(),
            "fe80::1234".parse().unwrap(),
            "fd00::4".parse().unwrap(),
            "192.168.7.8".parse().unwrap(),
            "10.0.0.8".parse().unwrap(),
        ];
        assert_eq!(
            choose_destination("0.0.0.0".parse().unwrap(), addresses.clone()),
            "10.0.0.8".parse::<IpAddr>().unwrap()
        );
        assert_eq!(
            choose_destination("::".parse().unwrap(), addresses),
            "fd00::4".parse::<IpAddr>().unwrap()
        );
        assert_eq!(
            choose_destination("::".parse().unwrap(), Vec::new()),
            IpAddr::V6(Ipv6Addr::LOCALHOST)
        );
    }

    #[test]
    fn explicit_bind_preserves_port_ipv6_and_loopback_scope() {
        for (bind, expected, scope) in [
            ("127.0.0.1:4567", "http://127.0.0.1:4567/", "loopback"),
            ("[::1]:4567", "http://[::1]:4567/", "loopback"),
            ("[fd00::42]:4567", "http://[fd00::42]:4567/", "lan"),
            ("192.168.2.42:4567", "http://192.168.2.42:4567/", "lan"),
        ] {
            let (base, actual_scope) = local_webhook_base_url(bind.parse().unwrap()).unwrap();
            assert_eq!(base.as_str(), expected);
            assert_eq!(actual_scope, scope);
        }
    }
    #[test]
    fn explicit_scoped_ipv6_bind_reports_unsupported_zone_instead_of_losing_it() {
        let bind = SocketAddr::V6(std::net::SocketAddrV6::new(
            "fe80::1234".parse().unwrap(),
            17878,
            0,
            7,
        ));
        let error = local_webhook_base_url(bind).unwrap_err();
        assert!(error.to_string().contains("zone identifier"));
    }
}
