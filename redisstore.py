"""
A redis-based store for TiddlyWeb. First thing we need to do is
come up with keys to use.

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
    tid:#tid:revisions (ordered) list of rids

    tiddler:#bag_name:#tiddler_name:tid: map bag+tiddler to tid

tiddler revisions:
    ids:nextRevisionID:the counter of revision ids

    rid:#rid:text:    tiddler text
    rid:#rid:tags:    list of tags # should have reverse index for this
    rid:#rid:modified:
    rid:#rid:modifier:
    rid:#rid:fields:  hash 
    rid:#rid:tid:     tid of this

"""

import redis

from tiddlyweb.model.bag import Bag
from tiddlyweb.model.policy import Policy
from tiddlyweb.model.recipe import Recipe
from tiddlyweb.model.tiddler import Tiddler
from tiddlyweb.model.user import User
from tiddlyweb.store import (NoBagError, NoTiddlerError, NoUserError,
        NoRecipeError)
from tiddlyweb.stores import StorageInterface

R = None

class Store(StorageInterface):

    def __init__(self, store_config=None, environ=None):
        global R
        super(Store, self).__init__(store_config, environ)
        if not R:
            R = redis.Redis()
        self.redis = R

    def bag_delete(self, bag):
        bid = self.redis.get('bag:%s:bid' % bag.name)
        if not bid:
            raise NoBagError('unable to get id for %s' % bag.name)
        tiddler_ids = list(self.redis.smembers('bid:%s:tiddlers' % bid))
        for tid in tiddler_ids:
            title = self.redis.get('tid:%s:title' % tid)
            tiddler = Tiddler(title, bag.name)
            self.tiddler_delete(tiddler)
        self.redis.delete('bid:%s:tiddlers' % bid)
        self.redis.delete('bag:%s:bid' % bag.name)
        pid = self.redis.get('bid:%s:policy' % bid)
        self._delete_policy(pid)
        self.redis.srem('bags', bid)

    def bag_get(self, bag):
        bid = self.redis.get('bag:%s:bid' % bag.name)
        if not bid:
            raise NoBagError('unable to get id for %s' % bag.name)

        bag.desc = self.redis.get('bid:%s:desc' % bid)
        bag.policy = self._get_policy('bid:%s:policy' % bid)

        return bag

    def recipe_delete(self, recipe):
        rid = self.redis.get('recipe:%s:rid' % recipe.name)
        if not rid:
            raise NoRecipeError('unable to get id for %s' % recipe.name)
        self.redis.delete('recipe:%s:rid' % recipe.name)
        self.redis.delete('rid:%s:name' % rid)
        self.redis.delete('rid:%s:desc' % rid)
        self.redis.delete('rid:%s:rlist' % rid)
        pid = self.redis.get('rid:%s:policy' % rid)
        self._delete_policy(pid)
        self.redis.srem('recipes', rid)

    def recipe_get(self, recipe):
        rid = self.redis.get('recipe:%s:rid' % recipe.name)
        if not rid:
            raise NoRecipeError('unable to get id for %s' % recipe.name)

        recipe.desc = self.redis.get('rid:%s:desc' % rid)
        recipe.policy = self._get_policy('rid:%s:policy' % rid)

        recipe_list = self.redis.lrange('rid:%s:rlist' % rid, 0, -1)

        recipe_items = []
        for bag_filter in recipe_list:
            bag, filter = bag_filter.split('?', 1)
            recipe_items.append((bag, filter))
        recipe.set_recipe(recipe_items)

        return recipe


    def recipe_put(self, recipe):
        rid = self.redis.get('recipe:%s:rid' % recipe.name)

        if not rid:
            rid = self.redis.incr('ids:nextRecipeID')
            self.redis.set('recipe:%s:rid' % recipe.name, rid)

        self.redis.set('rid:%s:name' % rid, recipe.name)
        self.redis.set('rid:%s:desc' % rid, recipe.desc)
        pid = self.redis.get('rid:%s:policy' % rid)
        pid = self._set_policy(recipe.policy, pid)
        self.redis.set('rid:%s:policy' % rid, pid)

        for bag, filter in recipe.get_recipe():
            self.redis.rpush('rid:%s:rlist' % rid, '%s?%s' % (bag, filter))

        self.redis.sadd('recipes', rid)


    def bag_put(self, bag):
        bid = self.redis.get('bag:%s:bid' % bag.name)

        if not bid:
            bid = self.redis.incr('ids:nextBagID')
            self.redis.set('bag:%s:bid' % bag.name, bid)

        self.redis.set('bid:%s:name' % bid, bag.name)
        self.redis.set('bid:%s:desc' % bid, bag.desc)

        pid = self.redis.get('bid:%s:policy' % bid)
        pid = self._set_policy(bag.policy, pid)
        self.redis.set('bid:%s:policy' % bid, pid)
        self.redis.sadd('bags', bid)

    def user_get(self, user):
        uid = self.redis.get('user:%s:uid' % user.usersign)
        if not uid:
            raise NoUserError('no user found for %s' % user.usersign)

        user._password = self.redis.get('uid:%s:password' % uid)
        user.note = self.redis.get('uid:%s:note' % uid)
        user.roles = list(self.redis.smembers('uid:%s:roles' % uid))
        return user

    def user_delete(self, user):
        uid = self.redis.get('user:%s:uid' % user.usersign)
        if not uid:
            raise NoUserError('no user found for %s' % user.usersign)
        self.redis.delete('uid:%s:usersign' % uid)
        self.redis.delete('uid:%s:password' % uid)
        self.redis.delete('uid:%s:roles' % uid)
        self.redis.delete('user:%s:uid' % user.usersign)
        self.redis.srem('users', uid)

    def user_put(self, user):
        uid = self.redis.get('user:%s:uid' % user.usersign)
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

    def tiddler_get(self, tiddler):
        tid = self.redis.get('tiddler:%s:%s:tid' % (tiddler.bag, tiddler.title))
        if not tid:
            raise NoTiddlerError('unable to load %s:%s'
                    % (tiddler.bag, tiddler.title))
        current_rid = self.redis.lindex('tid:%s:revisions' % tid, -1)
        base_rid = self.redis.lindex('tid:%s:revisions' % tid, 0)
        tiddler.creator = self.redis.get('rid:%s:modifier' % base_rid)
        tiddler.created = self.redis.get('rid:%s:modified' % base_rid)
        tiddler.modifier = self.redis.get('rid:%s:modifier' % current_rid)
        tiddler.modified = self.redis.get('rid:%s:modified' % current_rid)
        tiddler.tags = self.redis.smembers('rid:%s:tags' % current_rid)
        tiddler.fields = self.redis.hgetall('rid:%s:fields' % current_rid)
        tiddler.text = self.redis.get('rid:%s:text' % current_rid)
        return tiddler

    def tiddler_delete(self, tiddler):
        bid = self.redis.get('bag:%s:bid' % tiddler.bag)
        if not bid:
            raise NoBagError('no bag found: %s:%s'
                    % (tiddler.bag, tiddler.title))
        tid = self.redis.get('tiddler:%s:%s:tid' % (tiddler.bag, tiddler.title))
        if not tid:
            raise NoTiddlerError('no tiddler found: %s:%s'
                    % (tiddler.bag, tiddler.title))
        revision_ids = self.redis.lrange('tid:%s:revisions' % tid, 0, -1)
        delete_keys = []
        for rid in revision_ids:
            for field in ['text', 'tags', 'modified', 'modifier',
                    'fields', 'tid']:
                delete_keys.append('rid:%s:%s' % (rid, field))
        for field in ['title', 'bid', 'revisions']:
            delete_keys.append('tid:%s:%s' % (tid, field))
        delete_keys.append('tiddler:%s:%s:tid'
                % (tiddler.bag, tiddler.title))
        self.redis.delete(*delete_keys)
        self.redis.srem('bid:%s:tiddlers' % bid, tid)
        self.redis.delete('tiddler:%s:%s:tid' % (tiddler.bag, tiddler.title))

    def tiddler_put(self, tiddler):
        bid = self.redis.get('bag:%s:bid' % tiddler.bag)
        if not bid:
            raise NoBagError('No bag while trying to put tiddler: %s:%s' 
                    % (tiddler.bag, tiddler.title))
        tid = self.redis.get('tiddler:%s:%s:tid' % (tiddler.bag, tiddler.title))
        if not tid:
            tid = self.redis.incr('ids:nextTiddlerID')
            self.redis.set('tiddler:%s:%s:tid' % (tiddler.bag, tiddler.title),
                    tid)
            self.redis.set('tid:%s:title' % tid, tiddler.title)
            self.redis.set('tid:%s:bid' % tid, bid)
        rid = self._new_revision(tiddler, tid)
        self.redis.rpush('tid:%s:revisions' % tid, rid)
        self.redis.sadd('bid:%s:tiddlers' % bid, tid)

    def list_users(self):
        uids = self.redis.smembers('users')
        for uid in uids:
            name = self.redis.get('uid:%s:usersign' % uid)
            yield User(name)

    def list_bags(self):
        bids = self.redis.smembers('bags')
        for bid in bids:
            name = self.redis.get('bid:%s:name' % bid)
            yield Bag(name)

    def list_recipes(self):
        rids = self.redis.smembers('recipes')
        for rid in rids:
            name = self.redis.get('rid:%s:name' % rid)
            yield Recipe(name)

    def list_bag_tiddlers(self, bag):
        bid = self.redis.get('bag:%s:bid' % bag.name)
        if not bid:
            raise NoBagError('No bag while trying to list tiddlers: %s' 
                    % bag.name)
        tids = self.redis.smembers('bid:%s:tiddlers' % bid)
        for tid in tids:
            title = self.redis.get('tid:%s:title' % tid)
            yield Tiddler(title, bag.name)

    def list_tiddler_revisions(self, tiddler):
        tid = self.redis.get('tiddler:%s:%s:tid' % (tiddler.bag, tiddler.title))
        if not tid:
            raise NoTiddler('no such tiddler: %s:%s'
                    % (tiddler.bag, tiddler.title))
        revisions = self.redis.lrange('tid:%s:revisions' % tid, 0, -1)
        revisions.reverse()
        return revisions

    def _new_revision(self, tiddler, tid):
        rid = self.redis.incr('ids:nextRevisionID')
        self.redis.set('rid:%s:text' % rid, tiddler.text)
        self.redis.set('rid:%s:modifier' % rid, tiddler.modifier)
        self.redis.set('rid:%s:modified' % rid, tiddler.modified)
        self.redis.set('rid:%s:tid' % rid, tid)
        for tag in tiddler.tags:
            self.redis.sadd('rid:%s:tags' % rid, tag)
        if tiddler.fields:
            self.redis.hmset('rid:%s:fields' % rid, tiddler.fields)
        return rid

    def _get_policy(self, key):
        pid = self.redis.get(key)
        policy = Policy()
        if not pid:
            return policy
        for constraint in ['manage', 'accept', 'create', 'read', 'write']:
            key = 'pid:%s:%s' % (pid, constraint)
            values = self.redis.smembers(key)
            setattr(policy, constraint, list(values))
        policy.owner = self.redis.get('pid:%s:owner' % pid)
        return policy

    def _set_policy(self, container_policy, pid):
        if not pid:
            pid = self.redis.incr('ids:nextPolicyID')
        for constraint in ['manage', 'accept', 'create', 'read', 'write']:
            key = 'pid:%s:%s' % (pid, constraint)
            for member in getattr(container_policy, constraint):
                self.redis.sadd(key, member)
        self.redis.set('pid:%s:owner' % pid, container_policy.owner)
        return pid

    def _delete_policy(self, pid):
        for item in Policy.attributes:
            key = 'pid:%s:%s' % (pid, item)
            self.redis.delete(key)
        



