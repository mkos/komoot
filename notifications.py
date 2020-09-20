import fire
import pandas as pd
import datetime as dt


def bundle_notifications(input_path, output_path, typ='exact'):

    # read data
    df = pd.read_csv(input_path,
                     names=['timestamp', 'user_id', 'friend_id', 'friend_name'],
                     parse_dates=['timestamp']
                     )

    preproc = preprocessing(df)

    if typ == 'exact':
        bundled = bundle_exact(preproc)
    elif typ == 'predict':
        bundled = bundle_predict(preproc)
    else:
        "Unknown 'typ' parameter"

    final = postprocessing(bundled)

    # write data
    final.to_csv(output_path, header=True, index=False)


def preprocessing(df):
    df['date'] = df.timestamp.dt.strftime('%Y-%m-%d')
    df['prev_timestamp'] = df.groupby(['date', 'user_id']).timestamp.shift(1)
    df.prev_timestamp.fillna(value=df.timestamp, inplace=True)
    df['diff'] = (df.timestamp - df.prev_timestamp).dt.round('min')
    df['diff_m'] = (df.timestamp - df.prev_timestamp).dt.seconds // 60
    return df


def postprocessing(df):

    def make_message(row):
        if row['num_friends'] == 1:
            return '{} went on a tour'.format(row['first_friend_name'])
        else:
            return '{} and {} other went on a tour'.format(row['first_friend_name'], row['num_friends'] - 1)

    pre = \
        (df
         .sort_values('timestamp')
         .groupby(['user_id', 'notification_sent'])
         .agg({'timestamp': ['first', 'count'], 'friend_name': ['nunique', 'first']})
         .reset_index()
         )

    pre.columns = ['receiver_id', 'notification_sent', 'timestamp_first_tour', 'tours', 'num_friends',
                    'first_friend_name']

    final = \
        (pre
         .assign(message=pre.apply(make_message, axis=1))
         .drop(['first_friend_name', 'num_friends'], axis=1)
         )

    return final


def bundle_exact(df):

    def decision_function(ts):
        if ts.hour <= 8:
            return dt.datetime(ts.year, ts.month, ts.day, 9, 0, 0)
        elif ts.hour <= 10:
            return dt.datetime(ts.year, ts.month, ts.day, 11, 0, 0)
        elif ts.hour <= 15:
            return dt.datetime(ts.year, ts.month, ts.day, 16, 0, 0)
        elif ts.hour <= 20:
            return dt.datetime(ts.year, ts.month, ts.day, 21, 0, 0)
        else:
            next_day = ts + dt.timedelta(days=1)  # send all late evening notification next day
            return dt.datetime(next_day.year, next_day.month, next_day.day, 9, 0, 0)

    df['notification_sent'] = df.timestamp.apply(decision_function)

    return df


def bundle_predict(df):
    num_friends = df.groupby('user_id').friend_id.nunique()

    df = \
        (df
         .merge(num_friends.reset_index()
                .rename({'friend_id': 'friend_count'}, axis=1), on='user_id', how='left')
         )

    def param_decision_function(row, A, B, C, D):
        h = row['timestamp'].hour
        fc = row['friend_count']

        if 5 < h < 20:
            if fc < 5:
                return A
            else:
                return B
        else:
            if fc < 5:
                return C
            else:
                return D


    def decision_function(row):
        return param_decision_function(row, 37, 98, 22, 49)

    df['threshold_value'] = df.apply(decision_function, axis=1)

    df1 = df.copy()

    # setting the threshold
    df1['threshold_met'] = (df1.diff_m >= df1.threshold_value).astype(int)

    # creating bundles; this is the most time consuming part; takes >1m on the entire dataset
    bundles = \
        (df1
         .groupby(['user_id', 'date'])
         .threshold_met
         .expanding()
         .sum()
         )

    bundles.reset_index(level=[0, 1])['threshold_met'].astype(int)
    df1['bundle'] = bundles.reset_index(level=[0, 1])['threshold_met'].astype(int)

    # calculating bundle properties: # of tours, friend name from first notification, the bundle send time
    # and how much time passed from first tour in the bundle until bundle is sent
    bundle_props = \
        (df1
         .sort_values('timestamp')
         .groupby(['user_id', 'date', 'bundle'])
         .agg({'timestamp': ['min', 'max'], 'threshold_value': 'max', 'friend_name': ['nunique', 'first']})
         .assign(
            notification_sent=lambda df: df.timestamp['max'] + pd.to_timedelta(df.threshold_value['max'], unit='m'),
            bundle_max_await_time=lambda df: df.timestamp['max'] - df.timestamp['min'] + pd.to_timedelta(
                df.threshold_value['max'], unit='m'))
         .drop(['timestamp', 'threshold_value'], axis=1)
         )

    bundle_props.columns = ['tours', 'first_tour_friend', 'notification_sent', 'bundle_max_await_time']
    bundle_props.reset_index(inplace=True)

    return df1.merge(bundle_props, how='inner', on=['user_id', 'date', 'bundle'])


if __name__ == '__main__':
    fire.Fire(bundle_notifications)