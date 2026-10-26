"""Overlay one live trading day on an immutable historical ladder read."""
def overlay_live(base,current,sessions,rows=None,suspended_codes=()):
    result=dict(base);cutoff=current['date']
    dates=sorted(set(base.get('dates',[]))|set(current['dates']))[-sessions:]
    visible_days=set(dates)
    sealed={n['id']:dict(n) for n in base.get('sealedStocks',[])}
    sealed.update((n['id'],n) for n in current.get('sealedStocks',[]))
    context={n['id']:dict(n) for n in base.get('contextStocks',[])}
    context.update((n['id'],n) for n in current.get('contextStocks',[]))
    sealed={k:n for k,n in sealed.items() if n['date'] in visible_days}
    context={k:n for k,n in context.items() if k not in sealed}
    for name in ('links','relimitLinks'):
        edges={(e['source'],e['target']):e for e in base.get(name,[])}
        edges.update(((e['source'],e['target']),e) for e in current.get(name,[]))
        if name=='links':
            promoted={e['source'] for e in edges.values() if e['status']=='promoted'}
            current_sources={e['source'] for e in current.get(name,[]) if e['status']!='promoted'}
            edges={k:e for k,e in edges.items() if e['status']=='promoted' or e['source'] not in promoted and (e['source'] not in current_sources or e in current.get(name,[]))}
        result[name]=list(edges.values())
    outcomes={n['id']:dict(n) for n in base.get('nodes',[]) if n['status']!='sealed'}
    outcomes.update((n['id'],n) for n in current.get('nodes',[]) if n['status']!='sealed')
    # A long halt can end today without producing another sealed node.
    # Advance those dated outcomes using today's quote and suspension facts.
    if rows is not None:
        current_sources={e['source'] for e in current.get('links',[])}
        for edge in result['links']:
            if edge['status']!='paused' or edge['source'] in current_sources:continue
            old=outcomes.get(edge['target']);source=sealed.get(edge['source']) or context.get(edge['source'])
            if not old or not source:continue
            raw=rows.get(source['code']);halted=source['code'] in suspended_codes
            if halted and raw and float(raw.get('volume') or 0)>0:halted=False
            status='paused' if halted else 'pending' if current['live'].get('mode')!='close' or raw is None else 'failed'
            target={**old,'id':cutoff+':'+source['code']+':'+status,'date':cutoff,'status':status,'pct':raw.get('pct_chg') if raw and not halted else None}
            outcomes[target['id']]=target;edge.update(target=target['id'],status=status)
    wanted={e['target'] for e in result['links'] if e['status']!='promoted'}
    result['nodes']=list(sealed.values())+[n for k,n in outcomes.items() if k in wanted]
    for name in ('unpromotedStocks','brokenStocks','relimitContextStocks','relimitBrokenStocks','suspensionCarry'):
        key=lambda n:(n.get('id') or (n.get('targetDate','')+':'+n.get('code','')))
        rows={key(n):n for n in base.get(name,[]) if n.get('date')!=cutoff}
        rows.update((key(n),n) for n in current.get(name,[]))
        result[name]=list(rows.values())
    result.update(date=cutoff,dates=dates,sealedStocks=list(sealed.values()),contextStocks=list(context.values()),
        previousDates={**base.get('previousDates',{}),**current.get('previousDates',{})},
        firstCounts={**base.get('firstCounts',{}),**current.get('firstCounts',{})},
        brokenDates=sorted(set(base.get('brokenDates',[]))|{cutoff}),
        live=current['live'],source='offline_limitup_ladder+online_overlay',precomputed=True,themeAsOfDate=cutoff)
    raw={n['id']:n for name in ('sealedStocks','contextStocks','relimitContextStocks') for n in result.get(name,[])}
    parent={k:k for k in raw}
    def root(k):
        while parent[k]!=k:parent[k]=parent[parent[k]];k=parent[k]
        return k
    for edge in result['links']:
        a=raw.get(edge['source']);b=raw.get(edge['target'])
        if edge['status']=='promoted' and a and b and a['code']==b['code'] and b['board']==a['board']+1:parent[root(a['id'])]=root(b['id'])
    peaks={}
    for key,n in raw.items():
        r=root(key);p=peaks.get(r)
        if p is None or (n['board'],n['date'])>(p['board'],p['date']):peaks[r]=n
    for key,n in raw.items():
        p=peaks[root(key)];n.setdefault('originalTheme',n['theme'])
        n.update(theme=p.get('originalTheme',p['theme']),themePeakDate=p['date'],themePeakBoard=p['board'],themeAsOfDate=cutoff)
    for edge in result['links']:
        if edge['status']!='promoted' and edge['target'] in outcomes and edge['source'] in raw:
            outcomes[edge['target']]['theme']=raw[edge['source']]['theme']
    for n in result.get('unpromotedStocks',[]):
        if n.get('sourceId') in raw:n['theme']=raw[n['sourceId']]['theme']
    result['themes']=[{'name':t} for t in sorted({n['theme'] for n in sealed.values()})]
    return result
