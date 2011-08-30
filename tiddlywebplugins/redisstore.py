"""
A redis-based store for TiddlyWeb. Uses keys as follows:

recipes:
    ids:nextRecipeID: the counter of recipe ids

    rid.#rid.name:    name of the recipe
    rid.#rid.desc:    description of the recipe
    rid.#rid.policy:  id of the policy
    rid.#rid.rlist:   list of recipe

    recipe.#name.rid: rid associated with recipe name
    recipes:          set of all recipe rids

policy:
    ids:nextPolicyID: the counter of policy ids

    pid.#pid:manage:  set of strings
    pid.#pid:accept:  set of strings
    pid.#pid:create:  set of strings
    pid.#pid:edit:    set of strings
    pid.#pid:write:   set of strings
    pid.#pid:owner:   string

    policy.#pid:holder: (recipe|bag):#(rid|bid)

bags:
    ids:nextBagID:    the counter of bag ids

    bid:#bid:name:    bag name
    bid:#bid:desc:    bag desc
    bid:#bid:policy:  id of the policy
    bid:#bid:tiddlers:set of tiddler ids

    bag:#name:bid:    bid associated with bag name
    bags:             set of all bag bids

users:
    ids:nextUserID:   the counter of user ids

    uid:#uid:usersign:    user name
    uid:#uid:password:    user password
    uid:#uid:roles:   set of roles

    user:#name:uid:   uid associated with user name
    users:            set of all user ids

tiddlers:
    ids:nextTiddlerID:the counter of tiddler ids

    tid:#tid:title:   tiddler title
    tid:#tid:bid:     bag id
    tid:#tid:revisions (ordered) list of rvids

    tiddler:#bag_name:#tiddler_name:tid: map bag+tiddler to tid

tiddler revisions:
    ids:nextRevisionID:the counter of revision ids

    rvid:#rvid:text:    tiddler text
    rvid:#rvid:tags:    list of tags # should have reverse index for this
    rvid:#rvid:modified:
    rvid:#rvid:modifier:
    rvid:#rvid:fields:  hash
    rvid:#rvid:tid:     tid of this
"""

from redis.client import Redis

from tiddlyweb.util import binary_tiddler
from tiddlyweb.model.bag import Bag
from tiddlyweb.model.policy import Policy
from tiddlyweb.model.recipe import Recipe
from tiddlyweb.model.tiddler import Tiddler
from tiddlyweb.model.user import User
from tiddlyweb.store import (NoBagError, NoTiddlerError, NoUserError,
        NoRecipeError)
from tiddlyweb.stores import StorageInterface

R = None

ENTITY_MAP = {
        'user': 'uid',
        'recipe': 'rid',
        'bag': 'bid',
        }

class URedis(Redis):
    """
    Add some better unicode handling to the default redis class.
    This works for us because we only care about unicode strings
    anyway.
    """

    def __init__(self, *args, **kwargs):
        self.encoding = 'utf-8'
        Redis.__init__(self, *args, **kwargs)

    def uget(self, name):
        """
        Return the value and key ``name`` or None, and decode it if not None.
        """
        value = Redis.get(self, name)
        if value:
            return value.decode(self.encoding)
        return value

    def lrange(self, name, start, end):
        """
        Return a slice of the list ``name`` between
        position ``start`` and ``end``
        
        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation

        decode the results
        """
        results = Redis.lrange(self, name, start, end)
        return [result.decode(self.encoding) for result in results]

    def hgetall(self, name):
        """
        Return a Python dict of the hash's name/value pairs, both key and
        value decoded.
        """
        output = {}
        info = Redis.hgetall(self, name)
        for key, value in info.iteritems():
            output[key.decode(self.encoding)] = value.decode(self.encoding)
        return output

    def smembers(self, name):
        """
        Return all members of the set ``name``, decoding.
        """
        values = Redis.smembers(self, name)
        return (value.decode(self.encoding) for value in values)


class Store(StorageInterface):

    def __init__(self, store_config=None, environ=None):
        global R
        super(Store, self).__init__(store_config, environ)
        if not R:
            R = URedis(**store_config)
        self.redis = R

    def bag_delete(self, bag):
        bid = self._id_for_entity('bag', bag.name)
        if not bid:
            raise NoBagError('unable to get id for %s' % bag.name)

        self._delete_bag_tiddlers(bag.name, bid)

        delete_keys = []
        for key_name in ['tiddlers', 'name', 'desc']:
            delete_keys.append('bid:%s:%s' % (bid, key_name))
        delete_keys.append('bag:%s:bid' % bag.name)
        self.redis.delete(*delete_keys)

        pid = self.redis.uget('bid:%s:policy' % bid)
        self._delete_policy(pid)

        self.redis.srem('bags', bid)

    def bag_get(self, bag):
        bid = self._id_for_entity('bag', bag.name)
        if not bid:
            raise NoBagError('unable to get id for %s' % bag.name)

        bag.desc = self.redis.uget('bid:%s:desc' % bid)
        bag.policy = self._get_policy('bid:%s:policy' % bid)

        return bag

    def bag_put(self, bag):
        bid = self._id_for_entity('bag', bag.name)

        if not bid:
            bid = self.redis.incr('ids:nextBagID')
            self.redis.set('bag:%s:bid' % bag.name, bid)

        self.redis.set('bid:%s:name' % bid, bag.name)
        self.redis.set('bid:%s:desc' % bid, bag.desc)

        pid = self.redis.uget('bid:%s:policy' % bid)
        pid = self._set_policy(bag.policy, pid)
        self.redis.set('bid:%s:policy' % bid, pid)

        self.redis.sadd('bags', bid)

    def recipe_delete(self, recipe):
        rid = self._id_for_entity('recipe', recipe.name)
        if not rid:
            raise NoRecipeError('unable to get id for %s' % recipe.name)

        delete_keys = []
        for key_name in ['name', 'desc', 'rlist']:
            delete_keys.append('rid:%s:%s' % (rid, key_name))
        delete_keys.append('recipe:%s:rid' % recipe.name)
        self.redis.delete(*delete_keys)

        pid = self.redis.uget('rid:%s:policy' % rid)
        self._delete_policy(pid)

        self.redis.srem('recipes', rid)

    def recipe_get(self, recipe):
        rid = self._id_for_entity('recipe', recipe.name)
        if not rid:
            raise NoRecipeError('unable to get id for %s' % recipe.name)

        recipe.desc = self.redis.uget('rid:%s:desc' % rid)
        recipe.policy = self._get_policy('rid:%s:policy' % rid)

        recipe_list = self.redis.lrange('rid:%s:rlist' % rid, 0, -1)

        recipe_items = []
        for bag_filter in recipe_list:
            bag, filter_string = bag_filter.split('?', 1)
            recipe_items.append((bag, filter_string))
        recipe.set_recipe(recipe_items)

        return recipe

    def recipe_put(self, recipe):
        rid = self._id_for_entity('recipe', recipe.name)

        if not rid:
            rid = self.redis.incr('ids:nextRecipeID')
            self.redis.set('recipe:%s:rid' % recipe.name, rid)

        self.redis.set('rid:%s:name' % rid, recipe.name)
        self.redis.set('rid:%s:desc' % rid, recipe.desc)

        pid = self.redis.uget('rid:%s:policy' % rid)
        pid = self._set_policy(recipe.policy, pid)
        self.redis.set('rid:%s:policy' % rid, pid)

        for bag, filter_string in recipe.get_recipe():
            self.redis.rpush('rid:%s:rlist' % rid, '%s?%s'
                    % (bag, filter_string))

        self.redis.sadd('recipes', rid)

    def tiddler_delete(self, tiddler):
        bid = self._id_for_entity('bag', tiddler.bag)
        if not bid:
            raise NoBagError('no bag found: %s:%s'
                    % (tiddler.bag, tiddler.title))
        tid = self._tid_for_tiddler(tiddler)
        if not tid:
            raise NoTiddlerError('no tiddler found: %s:%s'
                    % (tiddler.bag, tiddler.title))

        revision_ids = self.redis.lrange('tid:%s:revisions' % tid, 0, -1)
        delete_keys = []
        for rvid in revision_ids:
            for field in ['text', 'tags', 'modified', 'modifier',
                    'fields', 'tid']:
                delete_keys.append('rvid:%s:%s' % (rvid, field))
        for field in ['title', 'bid', 'revisions']:
            delete_keys.append('tid:%s:%s' % (tid, field))
        delete_keys.append('tiddler:%s:%s:tid'
                % (tiddler.bag, tiddler.title))
        self.redis.delete(*delete_keys)

        self.redis.srem('bid:%s:tiddlers' % bid, tid)
        self.redis.delete('tiddler:%s:%s:tid' % (tiddler.bag, tiddler.title))

    def tiddler_get(self, tiddler):
        tid = self._tid_for_tiddler(tiddler)
        if not tid:
            raise NoTiddlerError('unable to load %s:%s'
                    % (tiddler.bag, tiddler.title))
        if tiddler.revision:
            current_rvid = tiddler.revision
        else:
            current_rvid = self.redis.lindex('tid:%s:revisions' % tid, -1)
        base_rvid = self.redis.lindex('tid:%s:revisions' % tid, 0)
        tiddler.creator = self.redis.uget('rvid:%s:modifier' % base_rvid)
        tiddler.created = self.redis.uget('rvid:%s:modified' % base_rvid)
        tiddler.modifier = self.redis.uget('rvid:%s:modifier' % current_rvid)
        if not tiddler.modifier:
            raise NoTiddlerError('unable to load %s:%s@%s'
                    % (tiddler.bag, tiddler.title, current_rvid))
        tiddler.modified = self.redis.uget('rvid:%s:modified' % current_rvid)
        tiddler.type = self.redis.uget('rvid:%s:type' % current_rvid)
        tiddler.tags = list(self.redis.smembers('rvid:%s:tags' % current_rvid))
        tiddler.fields = self.redis.hgetall('rvid:%s:fields' % current_rvid)
        if binary_tiddler(tiddler):
            tiddler.text = self.redis.get('rvid:%s:text' % current_rvid)
        else:
            tiddler.text = self.redis.uget('rvid:%s:text' % current_rvid)
        tiddler.revision = current_rvid
        return tiddler

    def tiddler_put(self, tiddler):
        bid = self._id_for_entity('bag', tiddler.bag)
        if not bid:
            raise NoBagError('No bag while trying to put tiddler: %s:%s'
                    % (tiddler.bag, tiddler.title))
        tid = self._tid_for_tiddler(tiddler)
        if not tid:
            tid = self.redis.incr('ids:nextTiddlerID')
            self.redis.set('tiddler:%s:%s:tid' % (tiddler.bag, tiddler.title),
                    tid)
            self.redis.set('tid:%s:title' % tid, tiddler.title)
            self.redis.set('tid:%s:bid' % tid, bid)
        rvid = self._new_revision(tiddler, tid)
        self.redis.rpush('tid:%s:revisions' % tid, rvid)
        self.redis.sadd('bid:%s:tiddlers' % bid, tid)

    def user_delete(self, user):
        uid = self._id_for_entity('user', user.usersign)
        if not uid:
            raise NoUserError('no user found for %s' % user.usersign)

        delete_keys = []
        for key_name in ['usersign', 'password', 'roles']:
            delete_keys.append('uid:%s:%s' % (uid, key_name))

        delete_keys.append('user:%s:uid' % user.usersign)

        self.redis.delete(*delete_keys)

        self.redis.srem('users', uid)

    def user_get(self, user):
        uid = self._id_for_entity('user', user.usersign)
        if not uid:
            raise NoUserError('no user found for %s' % user.usersign)

        user._password = self.redis.uget('uid:%s:password' % uid)
        user.note = self.redis.uget('uid:%s:note' % uid)
        user.roles = list(self.redis.smembers('uid:%s:roles' % uid))
        return user

    def user_put(self, user):
        uid = self._id_for_entity('user', user.usersign)
        if not uid:
            uid = self.redis.incr('ids:nextUserID')
            self.redis.set('user:%s:uid' % user.usersign, uid)

        self.redis.set('uid:%s:usersign' % uid, user.usersign)
        self.redis.set('uid:%s:password' % uid, user._password)
        self.redis.set('uid:%s:note' % uid, user.note)

        self.redis.delete('uid:%s:roles' % uid)
        for role in user.list_roles():
            self.redis.sadd('uid:%s:roles' % uid, role)

        self.redis.sadd('users', uid)

    def list_bags(self):
        bids = self.redis.smembers('bags')
        for bid in bids:
            name = self.redis.uget('bid:%s:name' % bid)
            yield Bag(name)

    def list_recipes(self):
        rids = self.redis.smembers('recipes')
        for rid in rids:
            name = self.redis.uget('rid:%s:name' % rid)
            yield Recipe(name)

    def list_users(self):
        uids = self.redis.smembers('users')
        for uid in uids:
            name = self.redis.uget('uid:%s:usersign' % uid)
            yield User(name)

    def list_bag_tiddlers(self, bag):
        bid = self._id_for_entity('bag', bag.name)
        if not bid:
            raise NoBagError('No bag while trying to list tiddlers: %s'
                    % bag.name)

        tids = self.redis.smembers('bid:%s:tiddlers' % bid)
        for tid in tids:
            title = self.redis.uget('tid:%s:title' % tid)
            yield Tiddler(title, bag.name)

    def list_tiddler_revisions(self, tiddler):
        tid = self._tid_for_tiddler(tiddler)
        if not tid:
            raise NoTiddlerError('no such tiddler: %s:%s'
                    % (tiddler.bag, tiddler.title))

        revisions = [int(i) for i in
                self.redis.lrange('tid:%s:revisions' % tid, 0, -1)]
        revisions.reverse()
        return revisions

    def _delete_bag_tiddlers(self, name, bid):
        tiddler_ids = list(self.redis.smembers('bid:%s:tiddlers' % bid))
        for tid in tiddler_ids:
            title = self.redis.uget('tid:%s:title' % tid)
            tiddler = Tiddler(title, name)
            self.tiddler_delete(tiddler)

    def _delete_policy(self, pid):
        for item in Policy.attributes:
            key = 'pid:%s:%s' % (pid, item)
            self.redis.delete(key)

    def _get_policy(self, key):
        pid = self.redis.uget(key)
        policy = Policy()
        if not pid:
            return policy
        for constraint in Policy.attributes:
            if constraint == 'owner':
                policy.owner = self.redis.uget('pid:%s:owner' % pid)
                if policy.owner == '':
                    policy.owner = None
            else:
                key = 'pid:%s:%s' % (pid, constraint)
                values = self.redis.smembers(key)
                setattr(policy, constraint, [value.decode(
                    self.redis.encoding) for value in values])
        return policy

    def _id_for_entity(self, entity, name):
        entity_id = ENTITY_MAP[entity]
        return self.redis.uget('%s:%s:%s' % (entity, name, entity_id))

    def _new_revision(self, tiddler, tid):
        rvid = self.redis.incr('ids:nextRevisionID')
        self.redis.set('rvid:%s:text' % rvid, tiddler.text)
        self.redis.set('rvid:%s:modifier' % rvid, tiddler.modifier)
        self.redis.set('rvid:%s:modified' % rvid, tiddler.modified)
        self.redis.set('rvid:%s:type' % rvid, tiddler.type)
        self.redis.set('rvid:%s:tid' % rvid, tid)
        for tag in tiddler.tags:
            self.redis.sadd('rvid:%s:tags' % rvid, tag)
        if tiddler.fields:
            stored_fields = {}
            for field in tiddler.fields.keys():
                if not field.startswith('server.'):
                    stored_fields[field] = tiddler.fields[field]
            self.redis.hmset('rvid:%s:fields' % rvid, stored_fields)
        return rvid

    def _set_policy(self, container_policy, pid):
        if not pid:
            pid = self.redis.incr('ids:nextPolicyID')
        for constraint in Policy.attributes:
            if constraint == 'owner':
                if container_policy.owner:
                    self.redis.set('pid:%s:owner' % pid, container_policy.owner)
                else:
                    self.redis.set('pid:%s:owner' % pid, '')
            else:
                key = 'pid:%s:%s' % (pid, constraint)
                self.redis.delete(key)
                for member in getattr(container_policy, constraint):
                    self.redis.sadd(key, member)
        return pid

    def _tid_for_tiddler(self, tiddler):
        return self.redis.uget('tiddler:%s:%s:tid'
                % (tiddler.bag, tiddler.title))
