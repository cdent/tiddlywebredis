
import py.test 

from tiddlyweb.control import get_tiddlers_from_recipe
from tiddlywebplugins.utils import get_store
from tiddlyweb.config import config
from tiddlyweb.model.bag import Bag
from tiddlyweb.model.tiddler import Tiddler
from tiddlyweb.model.recipe import Recipe
from tiddlyweb.model.user import User
from tiddlyweb.store import NoTiddlerError, NoBagError, NoUserError

import urllib

encoded_name = 'aaa%25%E3%81%86%E3%81%8F%E3%81%99'
name = urllib.unquote(encoded_name).decode('utf-8')

def setup_module(module):
    module.store = get_store(config)
    store.storage.redis.flushdb()
    module.environ = {'tiddlyweb.config': config}

def test_store_bag():
    bag = Bag(name)
    bag.desc = name
    bag.policy.accept.append('cdent')

    store.put(bag)

    bag2 = Bag(name)
    bag2 = store.get(bag2)

    assert bag.desc == bag2.desc
    assert bag.name == bag2.name
    assert bag.policy.accept == bag2.policy.accept

def test_store_tiddler():
    tiddler = Tiddler(name, name)
    tiddler.text = name
    tiddler.tags = [name]
    tiddler.fields['field key ' + name + ' one'] = name
    tiddler.fields['field key ' + name + ' two'] = name
    tiddler.modifier = name
    tiddler.modified = '20080202111111'

    store.put(tiddler)

    tiddler2 = Tiddler(name, name)
    tiddler2 = store.get(tiddler2)

    assert tiddler.text == tiddler2.text
    assert sorted(tiddler.tags) == sorted(tiddler2.tags)
    assert tiddler.fields['field key ' + name + ' two'] == tiddler2.fields['field key ' + name + ' two']
    assert tiddler.modifier == tiddler2.modifier
    assert tiddler.modified == tiddler2.modified

    tiddler2.text = 'pig'

    store.put(tiddler2)

    revisions = store.list_tiddler_revisions(tiddler2)
    assert len(revisions) == 2

    store.delete(tiddler2)

    py.test.raises(NoTiddlerError, 'store.get(tiddler2)')

    store.put(tiddler2)
    revisions = store.list_tiddler_revisions(tiddler2)
    assert len(revisions) == 1

    store.delete(Bag(name))

    py.test.raises(NoTiddlerError, 'store.get(tiddler2)')


def test_list_bag_tiddlers():
    bag = Bag(name)
    store.put(bag)

    tiddler = Tiddler('alpha', name)
    tiddler.text = 'alpha cow'
    store.put(tiddler)

    tiddler = Tiddler('beta', name)
    tiddler.text = 'beta cow'
    store.put(tiddler)

    tiddlers = list(store.list_bag_tiddlers(bag))
    assert len(tiddlers) == 2
    assert ['alpha', 'beta'] == sorted([tiddler.title for tiddler in tiddlers])

def test_list_bags():
    bag = Bag('testthree')
    store.put(bag)

    bags = list(store.list_bags())
    assert len(bags) == 2
    assert [name, 'testthree'] == sorted([bag.name for bag in bags])

def test_users():
    userc = User(name)
    userc.set_password(name)
    userc.add_role('ADMIN')
    userc.note = 'A simple programmer of matter'

    store.put(userc)

    user2 = store.get(User(name))
    assert user2.usersign == userc.usersign
    assert user2.check_password(name)
    assert user2.list_roles() == userc.list_roles()
    assert user2.note == userc.note


def test_recipes():
    store.put(Bag(name))
    store.put(Bag('beta'))

    tiddler = Tiddler('steak', name)
    tiddler.text = 'rare'
    store.put(tiddler)
    tiddler = Tiddler('liver', 'beta')
    tiddler.text = 'icky'
    store.put(tiddler)
    tiddler = Tiddler('steak', 'beta')
    tiddler.text = 'medium'
    store.put(tiddler)

    recipec = Recipe(name)
    recipec.desc = 'a meaty melange'
    recipec.policy.accept.append('cdent')
    recipec.set_recipe([
        (name, 'select=tag:systemConfig'),
        ('beta', '')])

    store.put(recipec)

    recipes = list(store.list_recipes())
    assert len(recipes) == 1
    reciped = store.get(recipes[0])

    assert reciped.name == recipec.name
    assert reciped.desc == recipec.desc
    recipe_list = reciped.get_recipe()

    assert recipe_list[0] == [name, 'select=tag:systemConfig']
    assert recipe_list[1] == ['beta', '']

    tiddlers = list(get_tiddlers_from_recipe(reciped, environ=environ))

    assert len(tiddlers) == 2
    for tiddler in tiddlers:
        assert tiddler.bag == 'beta'
        if tiddler.title == name:
            tiddler = store.get(tiddler)
            assert tiddler.text == 'medium'

    store.delete(reciped)

    recipes = list(store.list_recipes())
    assert len(recipes) == 0
