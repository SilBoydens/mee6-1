import os
import re
import requests
import gevent

from mee6 import Plugin
from mee6.discord import send_message
from mee6.types import MessageEmbed, Guild
from mee6.exceptions import APIException
from mee6.utils import timed
from modus import Model
from modus.exceptions import FieldValidationError
from modus.fields import String, Snowflake, List


class StreamerField(String):
    ERRORS = {'invalid_streamer': '"{0}" is not a valid streamer name'}
    @String.validator
    def validate_streamer(self, value):
        pattern = r'^[a-z0-9_]{3,25}$'
        if not re.match(pattern, value):
            msg = self.ERRORS['invalid_streamer'].format(value)
            raise FieldValidationError(msg) from None


class StreamersConfig(Model):
    announcement_message = String(max_length=1900)
    announcement_channel = Snowflake()
    hitbox_streamers = List(StreamerField())
    twitch_streamers = List(StreamerField())


class Streamers(Plugin):

    id = "streamers"
    name = "Streamers"
    description = "Get notified when your favourite twitch or hitbox streamer go live"
    config_model = StreamersConfig

    twitch_client_id = os.getenv('TWITCH_CLIENT_ID')

    streamer_rx = re.compile(r'^[a-z0-9_]{3,25}$')

    @Plugin.loop(sleep_time=0)
    def hitbox_loop(self):
        with timed('hitbox_live_delay'):
            offset = 0
            while 1:
                streams = self.get_hitbox_streams(offset=offset)
                if len(streams) == 0:
                    break

                for stream in streams:
                    gevent.spawn(self.handle_hitbox_stream, stream)

                offset += 100

                gevent.sleep(1)

    def get_hitbox_streams(self, offset=0):
        url = 'https://api.hitbox.tv/media/live/list.json'
        params = {'offset': offset,
                  'limit': 100}
        r = requests.get(url, params)
        if r.status_code != 200:
            return []

        data = r.json()
        if data.get('success', True) == False:
            return []

        livestreams = data['livestream']
        self.log('[Hitbox] Got {} streams ' \
                 '[offset={}]'.format(len(livestreams), offset))

        return livestreams

    def handle_hitbox_stream(self, stream):
        streamer = stream['media_name']
        key = 'plugin.{}.hitbox_streamer.{}.guilds'.format(self.id, streamer)
        guilds_ids = self.db.smembers(key)
        for guild_id in guilds_ids:
            if not self.check_guild(guild_id):
                continue

            guild = Guild(id=guild_id, plugin=self)
            try:
                self.announce_hitbox(guild, stream)
            except Exception as e:
                self.log('[Hitbox] An Exception occured announcing stream {}, guild' \
                         ' {} {}'.format(streamer, guild.id, e))

    def announce_hitbox(self, guild, stream):
        stream_id = stream['media_id']
        check = guild.storage.sismember('announced_hitbox_streams', stream_id)
        if check:
            return

        channel = stream['channel']

        embed = MessageEmbed()
        embed.color = 0x99cc00
        embed.title = stream['media_status']
        embed.url = channel['channel_link']

        embed.author_name = stream['media_display_name']
        embed.author_icon_url = 'https://edge.sf.hitbox.tv' + channel['user_logo']
        embed.author_url = channel['channel_link']

        embed.thumbnail_url = 'https://edge.sf.hitbox.tv' + channel['user_logo']
        embed.thumbnail_proxy_url = 'https://edge.sf.hitbox.tv' + channel['user_logo']
        embed.thumbnail_width, embed.thumbnail_height = 100, 100

        embed.image_url = 'https://edge.sf.hitbox.tv' + stream['media_thumbnail']

        embed.footer_text = 'Hitbox.tv'

        game = stream.get('category_name', "")
        if game:
            embed.add_field('Played Game', game, True)

        embed.add_field('Viewers', stream['category_viewers'] or 0, True)

        message = guild.config.announcement_message
        message = message.replace('{streamer}', embed.author_name)
        message = message.replace('{link}', embed.url)

        self.log('[Hitbox] Announcing {} to {}'.format(embed.author_name, guild.id))
        try:
            send_message(guild.config.announcement_channel, message, embed=embed)
            guild.storage.sadd('announced_hitbox_streams', stream_id)
        except APIException as e:
            self.log('[Hitbox] An error occured {} {} {}'.format(e.status_code,
                                                                 e.error_code,
                                                                 e.payload))
            if e.status_code in (403, 404):
                self.log('[Hitbox] Disabling plugin for {}'.format(guild.id))
                self.disable(guild)

    @Plugin.loop(sleep_time=0)
    def twitch_loop(self):
        with timed('twitch_live_delay'):
            offset = 0
            while 1:
                streams = self.get_twitch_streams(offset=offset)
                if len(streams) == 0:
                    break

                gevent.spawn(self.handle_twitch_streams, streams)

                offset += 100
                gevent.sleep(1)

    def handle_twitch_streams(self, streams):
        for stream in streams:
            gevent.spawn(self.handle_twitch_stream, stream)

    def handle_twitch_stream(self, stream):
        streamer = stream['channel']['name']
        key = 'plugin.{}.twitch_streamer.{}.guilds'.format(self.id, streamer)
        guilds_ids = self.db.smembers(key)
        for guild_id in guilds_ids:
            if not self.check_guild(guild_id):
                continue

            guild = Guild(id=guild_id, plugin=self)
            try:
                self.announce_twitch(guild, stream)
            except Exception as e:
                self.log('[Twitch] An Exception occured announcing stream {}, guild' \
                         '{} {}'.format(stream['channel']['name'], guild.id, e))

    def announce_twitch(self, guild, stream):
        stream_id = stream['_id']
        check = guild.storage.sismember('announced_twitch_streams', stream_id)
        if check:
            return

        channel = stream['channel']

        embed = MessageEmbed()
        embed.color = 0x6441A4
        embed.title = channel['status']
        embed.url = channel['url']

        embed.author_name = channel['display_name']
        embed.author_icon_url = channel['logo']
        embed.author_url = channel['url']

        embed.thumbnail_url = channel['logo']
        embed.thumbnail_proxy_url = channel['logo']
        embed.thumbnail_width, embed.thumbnail_height = 100, 100

        embed.image_url = stream['preview']['medium']

        embed.footer_text = 'Twitch.tv'

        game = stream.get('game')
        if game:
            embed.add_field('Played Game', game, True)

        embed.add_field('Viewers', stream['viewers'], True)

        message = guild.config.announcement_message
        message = message.replace('{streamer}', embed.author_name)
        message = message.replace('{link}', embed.url)

        self.log('[Twitch] Announcing {} to {}'.format(embed.author_name, guild.id))
        try:
            send_message(guild.config.announcement_channel, message, embed=embed)
            guild.storage.sadd('announced_twitch_streams', stream_id)
        except APIException as e:
            self.log('[Twitch] An error occured {} {} {}'.format(e.status_code,
                                                                 e.error_code,
                                                                 e.payload))
            if e.status_code in (403, 404):
                self.log('[Twitch] Disabling plugin for {}'.format(guild.id))
                self.disable(guild)

    def get_twitch_streams(self, offset=0, with_count=False):
        url = 'https://api.twitch.tv/kraken/streams/'
        headers = {'Client-ID': self.twitch_client_id,
                   'Accept': 'application/vnd.twitchtv.v5+json'}
        params={'offset': offset,
                'limit': 100}

        r = requests.get(url, params=params, headers=headers, timeout=10)
        if r.status_code == 200:
            data = r.json()

            self.log('[Twitch] Got {} streams [offset={}]'.format(len(data['streams']), offset))

            if with_count:
                return (data['streams'], data['_total'])
            else:
                return data['streams']

        self.log('[Twitch] Couldn\'t fetch streams (offset: {}, status_code:{})'.format(offset, status_code))

        return None

    def get_default_config(self, guild_id):
        return StreamersConfig(twitch_streamers=[],
                               hitbox_streamers=[],
                               announcement_channel=guild_id,
                               announcement_message='Hey @everyone! {streamer}' \
                               ' is now live on {link} ! Go check it out 😉!')

    def before_config_patch(self, guild_id, old_config, new_config):
        for streamer in old_config.twitch_streamers:
            key = 'twitch_streamer.{}.guilds'.format(streamer)
            self.plugin_db.srem(key, guild_id)

        for streamer in old_config.hitbox_streamers:
            key = 'hitbox_streamer.{}.guilds'.format(streamer)
            self.plugin_db.srem(key, guild_id)

    def after_config_patch(self, guild_id, config):
        for streamer in config.twitch_streamers:
            key = 'twitch_streamer.{}.guilds'.format(streamer)
            self.plugin_db.sadd(key, guild_id)

        for streamer in config.hitbox_streamers:
            key = 'hitbox_streamer.{}.guilds'.format(streamer)
            self.plugin_db.sadd(key, guild_id)

