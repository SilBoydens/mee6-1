import re
import traceback
import json

from mee6.utils import get
from mee6.rpc import get_guild_member, get_guild
from mee6.command.utils import build_regex
from mee6.command import Response
from mee6.utils.redis import GroupKeys, PrefixedRedis
from functools import wraps
from modus import Model
from modus.fields import String, Boolean, Integer, List, Snowflake
from modus.exceptions import FieldValidationError


class CommandContext:
    def __init__(self, guild_id, message):
        self.guild_id = guild_id
        self.message = message

    @property
    def author(self):
        return get_guild_member(self.guild_id, self.message.author.id)

    @property
    def guild(self):
        return get_guild(self.guild_id)


class CommandMatch:
    def __init__(self, command, rx_match):
        self.command = command
        self.rx_match = rx_match
        self.arguments = [t(arg) for t, arg in zip(command.cast_to, rx_match.groups())]


class Cooldown(Integer):
    def __init__(self, **kwargs):
        kwargs['min'] = -1
        super(Cooldown, self).__init__(**kwargs)

    @Integer.validator
    def not_null(self, value):
        if value == 0:
            raise FieldValidationError('Cooldown cannot be null') from None


class CommandConfig(Model):
    enabled = Boolean(required=True, default=True)
    global_cooldown = Cooldown()
    personal_cooldown = Cooldown()
    allowed_roles = List(Snowflake())


class Command:
    @classmethod
    def register(cls, expression):
        def deco(f):
            command_info = {'expression': expression,
                            'callback': f,
                            'description': ''}
            f.command_info = command_info
            return f
        return deco

    @classmethod
    def description(cls, description):
        def deco(f):
            f.command_info['description'] = description
            return f
        return deco

    @classmethod
    def restrict_default(cls, f):
        f.command_info['restrict_default'] = True
        return f

    def to_dict(self, guild_id=None):
        dct = {'id': self.id,
               'name': self.name,
               'description': self.description,
               '_expression': self.expression}

        if guild_id is not None:
            dct['config'] = self.get_config(guild_id).serialize()

        return dct

    def __init__(self, expression=None, callback=None, require_roles=[],
                 description="", after_check=lambda _, __ : True, plugin=None,
                 restrict_default=False):
        self.name = callback.__name__
        self.id = 'command.{}.{}'.format(plugin.id, self.name)
        self.expression = expression
        self.callback = callback
        self.require_roles = require_roles
        self.description = description
        self.regex, self.cast_to = build_regex(self.expression)
        self.after_check = after_check
        self.restrict_default = restrict_default

        self.plugin = plugin

        self.command_db = PrefixedRedis(plugin.db, self.id + '.')
        self.config_db = GroupKeys(self.id + '.config', self.command_db,
                                   cache=plugin.in_bot)

    def default_config(self, guild):
        guild_id = get(guild, 'id', guild)

        default_config = {'allowed_roles': [],
                          'enabled': True,
                          'global_cooldown': -1,
                          'personal_cooldown': -1}

        if not self.restrict_default:
            default_config['allowed_roles'] = [guild_id]

        return CommandConfig(**default_config)

    def get_config(self, guild):
        guild_id = get(guild, 'id', guild)

        raw_config = self.config_db.get('config.{}'.format(guild_id))
        if raw_config is None:
            return self.default_config(guild)

        config = json.loads(raw_config)
        return CommandConfig(**config)

    def patch_config(self, guild, partial_new_config):
        guild_id = get(guild, 'id', guild)
        config = self.get_config(guild)
        for field_name in config.__class__._fields:
            new_value = partial_new_config.get(field_name)
            if new_value is not None:
                setattr(config, field_name, new_value)
        config.sanitize()
        config.validate()
        raw_config = json.dumps(config.serialize())
        self.config_db.set('config.{}'.format(guild_id), raw_config)
        return config

    def delete_config(self, guild):
        guild_id = get(guild, 'id', guild)
        self.config_db.delete('config.{}'.format(guild_id))

    def check_permission(self, ctx):
        member_permissions = ctx.author.guild_permissions

        if ( member_permissions >> 5 & 1 ) or ( member_permissions >> 3 & 1):
            return True

        if int(ctx.author.id) == int(ctx.guild.owner_id):
            return True

        config = self.get_config(ctx.guild)
        allowed_roles = config.allowed_roles
        for role in ctx.author.roles:
            role_id = get(role, 'id', role)
            if role_id in allowed_roles:
                return True

        return False

    def check_enabled(self, ctx):
        config = self.get_config(ctx.guild)
        return config.enabled

    def check_cooldown(self, ctx):
        config = self.get_config(ctx.guild)

        global_cooldown = config.global_cooldown
        if global_cooldown > -1:
            key = 'cooldown.{}'.format(ctx.guild.id)
            cd_check = self.command_db.get(key)
            if cb_check:
                return False

            self.command_db.setex(key, 1, global_cooldown)

        personal_cooldown = config.personal_cooldown
        if global_cooldown > -1:
            key = 'cooldown.{}.{}'.format(ctx.guild.id, ctx.author.id)
            cd_check = self.command_db.get(key)
            if cb_check:
                return False

            self.command_db.setex(key, 1, personal_cooldown)

        return True

    def check_match(self, msg):
        match = self.regex.match(msg)
        if not match:
            return None

        return CommandMatch(self, match)

    def execute(self, guild, message):
        match = self.check_match(message.content)
        if match is None:
            return

        ctx = CommandContext(guild, message)

        if not self.check_permission(ctx):
            return

        if not self.check_enabled(ctx):
            return

        if not self.check_cooldown(ctx):
            return

        if not self.after_check(self, ctx):
            return

        try:
            response = self.callback(ctx, *match.arguments)
        except Exception as e:
            response = Response.internal_error()
            traceback.print_exc()

        if response:
            return response.send(guild, message.channel_id)
