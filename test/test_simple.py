
import py.test 

from tiddlywebplugins.utils import get_store
from tiddlyweb.config import config
from tiddlyweb.model.bag import Bag
from tiddlyweb.model.tiddler import Tiddler
from tiddlyweb.store import NoTiddlerError, NoBagError

def setup_module(module):
    module.store = get_store(config)
    store.storage.redis.flushdb()

def test_store_bag():
    bag = Bag('testone')
    bag.desc = 'testone'
    bag.policy.accept.append('cdent')

    store.put(bag)

    bag2 = Bag('testone')
    bag2 = store.get(bag2)

    assert bag.desc == bag2.desc
    assert bag.name == bag2.name
    assert bag.policy.accept == bag2.policy.accept

def test_store_tiddler():
    tiddler = Tiddler('monkey', 'testone')
    tiddler.text = 'cow'
    tiddler.tags = ['tagone', 'tagtwo', 'tagthree']
    tiddler.fields['field key one'] = 'fieldvalueone'
    tiddler.fields['field key two'] = 'fieldvaluetwo'
    tiddler.modifier = 'cdent'
    tiddler.modified = '20080202111111'

    store.put(tiddler)

    tiddler2 = Tiddler('monkey', 'testone')
    tiddler2 = store.get(tiddler2)

    assert tiddler.text == tiddler2.text
    assert sorted(tiddler.tags) == sorted(tiddler2.tags)
    assert tiddler.fields['field key two'] == tiddler2.fields['field key two']
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

    store.delete(Bag('testone'))

    py.test.raises(NoTiddlerError, 'store.get(tiddler2)')
