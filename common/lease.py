import os, time, datetime

LEASE_ID = os.getenv('LEASE_DOC_ID', 'bully')
OWNER = os.getenv('LEASE_OWNER', os.getenv('NODE_ID', '1'))

def _epoch(v):
    if isinstance(v, datetime.datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=datetime.timezone.utc)
        return v.timestamp()
    v = float(v)
    return v / 1000 if v > 1e11 else v

def lease_vigente(d):
    doc = d.coordinacion.find_one({'_id': LEASE_ID})
    if doc and str(doc.get('owner')) == OWNER and _epoch(doc['lease_until']) > time.time():
        return doc
    return None

