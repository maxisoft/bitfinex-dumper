#!/bin/sh
set -x
PUID=${PUID:-912}
usermod -u "$PUID" "${BITFINEX_USER_NAME:-bitfinexdumper}"
PGID=${PGID:-912}
groupmod -g "$PGID" "${BITFINEX_GROUP_NAME:-bitfinexdumper}"

BITFINEX_DEST_FOLDER=${BITFINEX_DEST_FOLDER:-/bitfinexdumper}
# create folders
if [ ! -d "${BITFINEX_DEST_FOLDER}" ]; then \
    mkdir -p "${BITFINEX_DEST_FOLDER}"
    chown -R "$PUID:$PGID" "${BITFINEX_DEST_FOLDER}"
fi

# check permissions
if [ ! "$(stat -c %u "${BITFINEX_DEST_FOLDER}")" = "$PUID" ]; then
	echo "Change in ownership detected, please be patient while we chown existing files ..."
	chown "$PUID:$PGID" -R "${BITFINEX_DEST_FOLDER}"
fi

renice "+${NICE_ADJUSTEMENT:-1}" $$ || :
exec ionice -c "${IONICE_CLASS:-3}" -n "${IONICE_CLASSDATA:-7}" -t su-exec "$PUID:$PGID" "${BITFINEX_DUMPER_EXE:-bitfinex-dumper}" $@