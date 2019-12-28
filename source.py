import env
import sys
import os
import itertools
import numpy as np
import pandas as pd

def data(period):
    files = sorted([f for f in os.listdir(env.COMPILED_PATH) if f.endswith(f'_{period}.csv')])
    return pd.concat([pd.read_csv(os.path.join(env.COMPILED_PATH, f)) for f in files]).sort_values(['gid', 'home']).reset_index(drop=True)

def allData():
    i = np.array([[f[:4],f[5:-4]] for f in os.listdir(env.COMPILED_PATH) if f.endswith('.csv')], dtype=int)
    i = i[np.lexsort(i.T)]
    return {p:pd.concat([pd.read_csv(os.path.join(env.COMPILED_PATH, f'{y}_{p}.csv')) for y in i[:,0][i[:,1]==p]]).sort_values(['gid', 'home']).reset_index(drop=True) for p in np.unique(i[:,1])}

def calcFeatures(df):
    # expected win rate
    expected_win = 1 / 1 + (df['allowed'] / df['scored']) ** 2
    # basic win record
    winrate = df['wins']/df['n']
    # calculate log 5 probability
    log5 = pd.DataFrame({
        'gid':df['gid'],
        'home': df['home'],
        'a': winrate,
    }).merge(pd.DataFrame({
        'gid':df['gid'],
        'home': df['home'] ^ 1,
        'b': winrate,
    }), how='left', on=['gid','home'], validate='1:1')[['a','b']]
    log5 = (log5['a'] * (1 - log5['b'])) / (log5['a'] * (1 - log5['b']) + log5['b'] * (1 - log5['a']))
    # batting
    ab = df[['bO','bE','bK','bS','bD','bT','bHR']].sum(axis=1)
    pa = df[['bO','bE','bK','bS','bD','bT','bHR','bBB','bHBP','bSH','bSF','bI']].sum(axis=1)
    hits = df[['bS','bD','bT','bHR']].sum(axis=1)
    slg = (df['bS'] + 2 * df['bD'] + 3 * df['bT'] + 4 * df['bHR']) / ab
    obp = (hits + df[['bBB','bHBP']].sum(axis=1)) / (ab + df[['bBB','bHBP','bSF']].sum(axis=1))
    ops = slg + obp
    return pd.concat([df[['gid','team','gameNumber', 'home','spread','pn', 'dn']], pd.DataFrame({
        # scores
        'win': (df['spread'] > 0).astype(int),
        'win_rate': winrate,
        'expected_win': expected_win,
        'log5': log5,
        #'scored_rate': df['scored'] / (df['scored'] + df['allowed']),
        'lob_rate': df['lob'] / df['n'],
        'win_vs_rate': df['wins_vs'] / df['n_vs'],
        'scored_vs_rate': df['scored_vs'] / (df['scored_vs'] + df['allowed_vs']),
        # defense
        'e': df['dE'] / df['dn'],
        # batting
        'ba': hits / ab,
        'slg': slg,
        'obp': obp,
        'ops': ops,
        'rbi': df['bRBI'] / ab,
        'bb': df['bBB'] / pa,
        # pitching
        'era': (df['pER'] * 27) / df['pIP'],
        'k': df['pK'] / df['pBF'],
        'pbb': df['pBB'] / df['pBF'],
        'ptb': (df['pS'] + 2 * df['pD'] + 3 * df['pT'] + 4 * df['pHR']) / df['pBF'],
        'phits': df[['pS','pD','pT','pHR']].sum(axis=1) / df['pBF'],
    }, index=df.index)], axis=1).replace([np.inf, -np.inf], np.nan)

def features(period):
    return calcFeatures(data(period))

def allFeatures():
    return { p: calcFeatures(df) for p,df in allData().items() }
