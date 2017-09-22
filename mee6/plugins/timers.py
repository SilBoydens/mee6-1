from mee6 import Plugin
from mee6.discord import get_channel_messages, send_webhook_message, send_message, get_current_user
from time import time
from mee6.utils import timed
from mee6.exceptions import APIException
from gevent.lock import Semaphore
from random import randint
from datadog import statsd

import hashlib
import math
import traceback
import gevent


class Timers(Plugin):

    id = "timers"
    name = "Timers"
    description = "Send messages at specific intervals"

    _lock = Semaphore(value=10)

    timers_ids = {}
    guild_jobs = {}
    timers = []

    def get_default_config(self, guild_id):
        default_config = {'timers': []}
        return default_config

    def on_start(self):
        with self._lock():
            guilds = self.get_guilds()
            self.log('Got {} guilds'.format(len(guilds)))

            timers = [timer for guild in guilds for timer in guild.config.timers]
            for guild in guilds:
                timers += guild.config.timers





    @Plugin.loop(sleep_time=0)
    def loop(self):
        guilds = self.get_guilds()
        self.log('Got {} guilds'.format(len(guilds)))
        job_count = 0
        for guild in guilds:
            job = self.guild_jobs.get(guild.id)

            ready_to_launch = job is None or job.ready()
            if ready_to_launch:
                guild_config = guild.config

                # Ignoring 0 timers
                # So that we spawn less greenlets
                if guild_config['timers'] == []:
                    continue

                job = gevent.spawn(self.process_timers, guild, guild_config)

                self.guild_jobs[guild.id] = job

                job_count += 1

        self.log('Relaunched {} jobs out of {}'.format(job_count,
                                                       len(self.guild_jobs.keys())))

    def on_config_change(self, guild, config):
        self.log('Got guild config change for guild {}'.format(guild.id))
        job = self.guild_jobs.get(str(guild.id))
        if job:
            job.kill()

    def process_timers(self, guild, config):
        next_announces = []
        for timer in config['timers']:
            try:
                next_announce = self.process_timer(timer)
                next_announces.append(next_announce)
            except APIException as e:
                self.log('Got Api exception {} {}'.format(e.status_code, e.payload))

                # Disabling the plugin in case of an error
                # Unauthorized or channel not found
                self.log('Disabling plugin for {}'.format(guild.id))
                self.disable(guild)
                return

        next_announce = min(next_announces)
        sleep_time = max(0, math.floor(next_announce - time()))
        gevent.sleep(sleep_time)


    def get_timer_id(self, timer_message, timer_interval, channel):
        """ since storing timer ids on config is meh, i'm just using a hash.
        two identical timers will collision. But we don't care here."""

        phrase = '{}.{}.{}'.format(timer_message, timer_interval, channel)
        timer_id = self.timers_ids.get(phrase)
        if timer_id:
            return timer_id

        timer_id = hashlib.sha224(phrase.encode('utf-8')).hexdigest()
        self.timers_ids[phrase] = timer_id
        return timer_id

    def process_timer(self, timer):
        message = timer['message']
        channel = timer['channel']
        interval = timer['interval']
        timer_id = self.get_timer_id(message, interval, channel)

        last_post_timestamp = self.db.get('plugin.timers.{}.last_post_timestamp'.format(timer_id))
        last_post_timestamp = int(last_post_timestamp or 0)

        next_announce = last_post_timestamp + timer['interval']

        now = math.floor(time())
        if now < next_announce:
            return next_announce

        webhook_id = 'timers:{}'.format(channel)

        do_post = True

        key = 'channel.{}.last_message_whid'.format(channel)
        last_message_webhook_id = self.db.get(key)
        if self.db.sismember('plugin.timers.webhooks', last_message_webhook_id):
            do_post = False

        now = math.floor(time())

        self.db.set('plugin.timers.{}.last_post_timestamp'.format(timer_id), now)

        if do_post:
            post_message = send_webhook_message(webhook_id, channel, message)
            self.db.sadd('plugin.timers.webhooks', post_message.webhook_id)
            self.log('Announcing timer message ({} interval) in {}'.format(interval, channel))

            if last_post_timestamp != 0:
                statsd.timing('timers_delay', int(time()) - next_announce)

        return now + timer['interval']

