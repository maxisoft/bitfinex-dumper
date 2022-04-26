# BitFinex dumper systemd unit file
This readme explain how to install *bitfinexdumper* into a linux server using systemd.  
One may prefer the docker/podman way

## Installation into a system via systemd
```bash
bash
set -euo pipefail
wget "https://github.com/maxisoft/bitfinex_dumper/releases/download/v1.0.0/bitfinex_dumper" -O "/tmp/bitfinex_dumper"
useradd -r -s /bin/false bitfinexdumper # TODO checks that the added user doesn't break your server security (like ssh)
install -d -m 0755 -o bitfinexdumper -g bitfinexdumper /opt/bitfinexdumper
install -d -m 0755 -o bitfinexdumper -g bitfinexdumper /var/bitfinexdumper
install -m 0554 -o bitfinexdumper -g bitfinexdumper /tmp/bitfinex_dumper /opt/bitfinexdumper/bitfinex_dumper
touch /tmp/bitfinex.db
install -m 0644 -o bitfinexdumper -g bitfinexdumper /tmp/bitfinex.db /var/bitfinexdumper/bitfinex.db
# Disable cow for btrfs user: (it yield better performance but it's less safer for your data)
# chattr +C /var/bitfinexdumper/bitfinex.db

# optionally edit the bitfinexdumper.service file
cp bitfinexdumper.service /etc/systemd/system/bitfinexdumper.service
systemctl daemon-reload
systemctl start bitfinexdumper
# check running state with:
# systemctl status bitfinexdumper
systemctl enable bitfinexdumper

# cleanup
rm -f /tmp/bitfinex.db
rm -f /tmp/bitfinex_dumper
```

## Installation via podman
```bash
bash
set -euo pipefail
mkdir -p /var/bitfinexdumper
podman run -d --name bitfinexdumper -v /var/bitfinexdumper:/bitfinexdumper ghcr.io/maxisoft/bitfinex_dumper/bitfinex_dumper:latest
cp bitfinexdumper-pod.service /etc/systemd/system/bitfinexdumper-pod.service
systemctl daemon-reload
systemctl start bitfinexdumper-pod
# check running state with:
# systemctl status bitfinexdumper-pod
systemctl enable bitfinexdumper-pod

## Timer to compress databases
bash
set -euo pipefail
mkdir -p /opt/bitfinexdumper/scripts
cp ../scripts/compress_databases.py /opt/bitfinexdumper/scripts
chmod +x /opt/bitfinexdumper/scripts/compress_databases.py
cp bitfinexdumper-compress-databases.{service,timer} /etc/systemd/system/
