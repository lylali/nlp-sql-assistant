import math, re
TOKEN=re.compile(r"[a-z0-9]+")
def tok(x): return TOKEN.findall(x.lower())

def build(qs):
    N=len(qs); doc_tfs=[]; df={}
    for q in qs:
        tf={}
        for t in tok(q): tf[t]=tf.get(t,0)+1
        tflog={t:1.0+math.log(v) for t,v in tf.items()}
        doc_tfs.append(tflog)
        for t in tflog: df[t]=df.get(t,0)+1
    idf={t: math.log(N/(df_t+1))+1.0 for t,df_t in df.items()}
    return doc_tfs,idf

def cos(a,b,idf):
    dot=sum(a.get(t,0)*b.get(t,0)*idf.get(t,1)**2 for t in set(a)|set(b))
    na=math.sqrt(sum((a.get(t,0)*idf.get(t,1))**2 for t in a)) or 1.0
    nb=math.sqrt(sum((b.get(t,0)*idf.get(t,1))**2 for t in b)) or 1.0
    return dot/(na*nb)

def rank(query, corpus_qs, topk=3):
    doc_tfs,idf=build(corpus_qs)
    qtf={}
    for t in tok(query): qtf[t]=qtf.get(t,0)+1
    qlog={t:1.0+math.log(v) for t,v in qtf.items()}
    scores=[(i,cos(qlog,doc_tfs[i],idf)) for i in range(len(corpus_qs))]
    scores.sort(key=lambda x:x[1], reverse=True)
    return [i for i,_ in scores[:topk]]
