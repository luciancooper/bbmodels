import env
import sys
import os
import io
import tempfile
import itertools
import requests
import datetime
import numpy as np
import pandas as pd
from tqdm import tqdm

tqdm_kwargs = { 'ncols': 90, 'file': sys.stderr }

def getcsv(route):
    """
    Request csv data from endpoint and return as StringIO instance
    """
    # create request
    res = requests.get(f'{env.SOURCE_URL}/{route}', headers={'Accept':'text/csv'})
    # if request fails, report error
    if res.status_code != 200:
        print(f"HTTP [{res.status_code}] {route}", file=sys.stderr)
        return None
    # return StringIO instance
    return io.StringIO(res.text)

def _pitching(year, *periods):
    # stats we are going to output
    statkeys = ['W', 'L', 'SV', 'R', 'ER', 'IP', 'BF', 'S', 'D', 'T', 'HR', 'BB', 'HBP', 'IBB', 'K', 'BK', 'WP', 'PO', 'GDP']
    # fetch game starting pitcher data for the last two seasons
    starters = pd.concat([
        pd.read_csv(getcsv(f'lineups/{year-1}'), usecols=['gid', 'team', 'home', 'gameNumber', 'pitcher']),
        pd.read_csv(getcsv(f'lineups/{year}'), usecols=['gid', 'team', 'home', 'gameNumber', 'pitcher']),
    ]).rename(
        columns={'pitcher': 'pid'}
    ).sort_values(['gid', 'home']).drop(columns=['home'])
    # fetch pitching performance by game for the last two seasons
    pitching = pd.concat([pd.read_csv(getcsv(f'pitching/player/games/{y}')) for y in (year-1,year)])
    # merge starting pitcher data with pitching stats data
    pitching = starters.merge(
        pitching,
        how='left',
        on=['gid','team','gameNumber','pid'],
        validate='1:1',
    ).fillna(0)
    # extract numpy ndarrays
    index = pitching[['gid','team','gameNumber','pid']].values
    stats = pitching[statkeys].values.astype(int)
    # derive date column from game id
    dates = np.array([f'{gid[:4]}-{gid[4:6]}-{gid[6:8]}' for gid in index[:,0]], dtype='datetime64')
    # get mask for games only from current year
    mask = (dates >= datetime.date(year, 1, 1))
    # iterate through each period
    for period in periods:
        # initialize output row array
        output = []
        # iterate through season games
        for date,(gid, team, gameNumber, pid) in tqdm(zip(dates[mask], index[mask]), total = mask.sum(), desc = f'{year} ({period}) pitching', leave=True, **tqdm_kwargs):
            # fetch the starting pitchers past starts
            pastStarts = stats[
                ((dates < date) | ((dates == date) & (index[:,2] < gameNumber))) & (index[:,3] == pid)
            ][-period:]
            # the number of past starts found for the starting pitcher
            n = len(pastStarts)
            # append output row
            output.append([
                gid,
                team,
                gameNumber,
                pid,
                n,
                *(pastStarts.sum(axis=0) if n else [0]*len(statkeys))
            ])
        # yield period and dataframe
        yield (period, pd.DataFrame(
            output,
            columns=['gid', 'team', 'gameNumber', 'ppid', 'pn'] + [f'p{k}' for k in statkeys],
        ))

def _batting(year, *periods):
    # stats we are going to output
    statkeys = ['O', 'E', 'S', 'D', 'T', 'HR', 'BB', 'IBB', 'HBP', 'K', 'I', 'SH', 'SF', 'GDP', 'R', 'RBI', 'SB', 'CS', 'PO']
    # fetch game starting lineup data for the last two seasons
    starters = pd.concat([
        pd.read_csv(getcsv(f'lineups/{year-1}')).drop(['pitcher']+[f'pos{i}' for i in range(1,10)], axis=1),
        pd.read_csv(getcsv(f'lineups/{year}')).drop(['pitcher']+[f'pos{i}' for i in range(1,10)], axis=1),
    ])
    # retrieve games for iteration later
    games = starters.sort_values(['gid','home'])[['gid','team','gameNumber']]
    games = games[games.gid.str.startswith(str(year))].values
    # derive game date values
    gamedates = np.array([f'{gid[:4]}-{gid[4:6]}-{gid[6:8]}' for gid in games[:,0]], dtype='datetime64')
    # transform starting lineup dataframe
    starters = pd.concat([starters[['gid', 'home', 'team', 'gameNumber', f'pid{i}']].rename(
        columns={f'pid{i}':'pid'}
    ) for i in range(1, 10)]).sort_values(['gid', 'home']).drop(columns=['home'])
    # fetch batting performance by game for the last two seasons
    batting = pd.concat([
        pd.read_csv(getcsv(f'batting/player/games/{year-1}')),
        pd.read_csv(getcsv(f'batting/player/games/{year}')),
    ])
    # merge starting pitcher data with pitching stats data
    batting = starters.merge(
        batting,
        how='left',
        on=['gid','team','gameNumber','pid'],
        validate='1:1',
    ).fillna(0)
    # extract numpy ndarrays
    index = batting[['gid','team','gameNumber','pid']].values
    stats = batting[statkeys].values.astype(int)
    i = np.arange(0, len(index))
    # derive date column from game id
    dates = np.array([f'{gid[:4]}-{gid[4:6]}-{gid[6:8]}' for gid in index[:,0]], dtype='datetime64')
    # iterate through each period
    for period in periods:
        # initialize output row array
        output = []
        # iterate through season games
        for date,(gid, team, gameNumber) in tqdm(zip(gamedates, games), total = len(games), desc = f'{year} ({period}) batting ', leave=True, **tqdm_kwargs):
            pids = index[:,3][(index[:,0] == gid) & (index[:,1] == team)]
            mask = ~(index[:,0] != None)
            datemask = (dates < date) | ((dates == date) & (index[:,2] < gameNumber))
            for pid in pids:
                pidMask = datemask & (index[:,3] == pid)
                if pidMask.sum() > period:
                    pidMask = pidMask & (i >= i[pidMask][-period])
                mask = mask | pidMask
            # fetch the starting lineup players past starts
            pastStarts = stats[mask]
            # append output row
            output.append([
                gid,
                team,
                gameNumber,
                *(pastStarts.sum(axis=0) if len(pastStarts) else [0]*len(statkeys)),
            ])
        # return dataframe
        yield (period, pd.DataFrame(
            output,
            columns=['gid', 'team', 'gameNumber'] + [f'b{k}' for k in statkeys],
        ))

def _defense(year, *periods):
    # stats we are going to output
    statkeys = ['UR','TUR','P','A','E','PB']
    # fetching batting data by game
    defense = pd.read_csv(getcsv(f'defense/team/games/{year}'))
    # extract numpy arrays from pandas dataframe
    index = defense[['gid','team','gameNumber']].values
    stats = defense[statkeys].values.astype(int)
    # iterate through each period
    for period in periods:
        # initialize output row array
        output = []
        # iterate through batting data tuples
        for gid,team,gameNumber in tqdm(index, total = index.shape[0], desc = f'{year} ({period}) defense ', **tqdm_kwargs):
            # find past games and sum the data
            prevGames = stats[(index[:,2] < gameNumber) & (index[:,1] == team)][-period:]
            # the number of past games found for this team
            n = prevGames.shape[0]
            # append output row
            output.append([
                gid,
                team,
                gameNumber,
                n,
                *(prevGames.sum(axis=0) if n else [0]*len(statkeys))
            ])
        # return dataframe
        yield (period, pd.DataFrame(
            output,
            columns=['gid', 'team', 'gameNumber', 'dn'] + [f'd{k}' for k in statkeys],
        ))

def _scores(year, *periods):
    # fetch score data for the given season
    scores = pd.read_csv(getcsv(f'scores/{year}'))
    # get length of scores dataframe
    N = scores.values.shape[0]
    # extract numpy ndarray (object) from dataframe index columns
    index = scores[['gid','team','gameNumber','opp','home']].values
    # extract numpy ndarray (int) from dataframe data columns
    stats = np.concatenate([
        (scores['score'].values-scores['opp_score'].values).reshape((N, 1)),
        (scores['score'].values > scores['opp_score'].values).astype(int).reshape((N, 1)),
        scores[['score','opp_score','lob']].values.astype(int),
    ],axis=1)
    # iterate through each period
    for period in periods:
        # initialize output row array
        output = []
        # iterate through score rows
        for (gid,team,gameNumber,opp,home),spread in tqdm(zip(index,stats[:,0]), total = N, desc = f'{year} ({period}) scores  ', **tqdm_kwargs):
            # create previous games mask
            prev = (index[:,2] < gameNumber) & (index[:,1] == team)
            # extend mask to filter previous games vs current opponent
            prev_vs = prev & (index[:,3] == opp)
            # apply mask & get previous games stats
            prev = stats[prev][-period:]
            # apply mask & get previous games vs stats
            prev_vs = stats[prev_vs][-period:]
            # add row to output
            output.append([
                gid,
                team,
                gameNumber,
                opp,
                home,
                spread,
                prev.shape[0], # the number of past games found for this team
                prev[:,1].sum(), # the number of wins
                prev[:,2].sum(), # runs scored by this team
                prev[:,3].sum(), # runs scored against this team
                prev[:,4].sum(), # runners left on base
                prev_vs.shape[0], # the number of past games played against opponent
                prev_vs[:,1].sum(), # wins vs this opponent
                prev_vs[:,2].sum(), # runs scored by this team against this opponent
                prev_vs[:,3].sum(), # runs scored by this opponent against this team
            ])
        # return dataframe
        yield (period, pd.DataFrame(
            output,
            columns=['gid','team','gameNumber','opp','home','spread','n','wins', 'scored','allowed','lob','n_vs','wins_vs', 'scored_vs', 'allowed_vs']
        ))

def _merge(tempdir, year, period):
    merge_args = { 'how': 'inner', 'left_index': True, 'right_index': True }
    index = ['gid', 'team', 'gameNumber']
    return pd.read_csv(
        os.path.join(tempdir, f'{year}_{period}_scores.csv'), index_col=index,
    ).merge(
        pd.read_csv(os.path.join(tempdir, f'{year}_{period}_defense.csv'), index_col=index),
        ** merge_args,
    ).merge(
        pd.read_csv(os.path.join(tempdir, f'{year}_{period}_pitching.csv'), index_col=index),
        ** merge_args,
    ).merge(
        pd.read_csv(os.path.join(tempdir, f'{year}_{period}_batting.csv'), index_col=index),
        ** merge_args,
    )

def _compile(year, *periods):
    with tempfile.TemporaryDirectory() as tempdir:
        # scores
        for p, df in _scores(year, *periods):
            df.to_csv(os.path.join(tempdir, f'{year}_{p}_scores.csv'), index=False)
        # defense
        for p, df in _defense(year, *periods):
            df.to_csv(os.path.join(tempdir, f'{year}_{p}_defense.csv'), index=False)
        # pitching
        for p, df in _pitching(year, *periods):
            df.to_csv(os.path.join(tempdir, f'{year}_{p}_pitching.csv'), index=False)
        # batting
        for p, df in _batting(year, *periods):
            df.to_csv(os.path.join(tempdir, f'{year}_{p}_batting.csv'), index=False)
        # merge
        for p in periods:
            outfile = os.path.join(env.COMPILED_PATH, f'{year}_{p}.csv')
            _merge(tempdir, year, p).to_csv(outfile)
            print(f'wrote {outfile}')

def parse_ints(arg):
    ints = set()
    for a in arg.split(','):
        if '-' in a:
            i0,i1 = map(int,a.split('-'))
            ints |= set(range(i0,i1+1))
        else:
            ints |= {int(a)}
    ints = list(ints)
    ints.sort()
    return ints

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(prog='compiler',description='Compile dataset files')
    parser.add_argument('years',type=parse_ints,help='seasons to compile')
    parser.add_argument("periods",type=int, nargs='+', help='period values')
    args = parser.parse_args()
    for year in args.years:
        _compile(year, *args.periods)