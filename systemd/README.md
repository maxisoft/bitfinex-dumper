# BitFinex dumper systemd unit file

## Installation
```bash
bash
set -euo pipefail
wget "https://github.com/maxisoft/bitfinex_dumper/releases/download/v1.0.0/bitfinex_dumper" -O "/tmp/bitfinex_dumper"
useradd -r -s /bin/false bitfinexdumper
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
