from typing import Dict, Union, Any, Tuple

import pymysql
from dtmcli import barrier, tcc, utils, saga, msg
from flask import Flask, request
from pymysql.cursors import Cursor

app = Flask(__name__)
dbconf = {'host': '124.222.54.172', 'port': '3306', 'user': 'root', 'password': 'lhf19820130'}


def conn_new() -> Cursor:
    print('正在连接数据库：', dbconf)
    return pymysql.connect(host=dbconf['host'], user=dbconf['user'], password=dbconf['password'], database='').cursor()


def barrier_from_req(request: request):
    print('调用barrier_from_req()函数')
    return barrier.BranchBarrier(request.args.get('trans_type'), request.args.get('gid'), request.args.get('branch_id'),
                                 request.args.get('op'))


# 这是dtm服务地址
dtm: str = "http://localhost:36789/api/dtmsvr"
# 这是业务微服务地址
svc: str = "http://localhost:5000/api"

out_uid: int = 1
in_uid: int = 2


@app.get('/api/fireSaga')
def fire_saga() -> Dict[str, str]:
    print('调用fire_saga()函数，调用路径：/api/fireSaga，调用方式：get')
    req: Dict[str, int] = {'amount': 30}
    s: saga.Saga = saga.Saga(dtm, utils.gen_gid(dtm))
    s.add(req, svc + '/TransOutSaga', svc + '/TransOutCompensate')
    s.add(req, svc + '/TransInSaga', svc + '/TransInCompensate')
    s.submit()
    return {'gid': s.trans_base.gid}


def saga_adjust_balance(cursor, uid: int, amount: int) -> None:
    print('调用saga_adjust_balance()函数')
    affected: Any = utils.sqlexec(
        cursor,
        "update dtm_busi.user_account set balance=balance+%d where user_id=%d and balance >= -%d" % (
            amount, uid, amount)
    )
    if affected == 0:
        raise Exception("update error, balance not enough")


@app.post('/api/TransOutSaga')
def trans_out_saga():
    print('调用trans_out_saga()函数，调用路径：/api/TransOutSaga，调用方式：post')
    with barrier.AutoCursor(conn_new()) as cursor:
        def busi_callback(c):
            print('调用busi_callback()函数，上层调用函数：trans_out_saga')
            saga_adjust_balance(c, out_uid, -30)

        barrier_from_req(request).call(cursor, busi_callback)
    return {'dtm_result': 'SUCCESS'}


@app.post('/api/TransOutCompensate')
def trans_out_compensate():
    print('调用trans_out_compensate()函数，调用路径：/api/TransOutCompensate，调用方式：post')
    with barrier.AutoCursor(conn_new()) as cursor:
        def busi_callback(c):
            print('调用busi_callback()函数，上层调用函数：trans_out_compensate')
            saga_adjust_balance(c, out_uid, 30)

        barrier_from_req(request).call(cursor, busi_callback)
    return {"dtm_result": "SUCCESS"}


@app.post('/api/TransInSaga')
def trans_in_saga():
    print('调用trans_in_saga()函数，调用路径：/api/TransInSaga，调用方式：post')
    with barrier.AutoCursor(conn_new()) as cursor:
        def busi_callback(c):
            print('调用busi_callback()函数，上层调用函数：trans_in_saga')
            saga_adjust_balance(c, in_uid, 30)

        barrier_from_req(request).call(cursor, busi_callback)
    return {"dtm_result": "SUCCESS"}


@app.post('/api/TransInCompensate')
def trans_in_compensate():
    print('调用trans_in_compensate()函数，调用路径：/api/TransInCompensate，调用方式：post')
    with barrier.AutoCursor(conn_new()) as cursor:
        def busi_callback(c):
            print('调用busi_callback()函数，上层调用函数：trans_in_compensate')
            saga_adjust_balance(c, in_uid, -30)

        barrier_from_req(request).call(cursor, busi_callback)
    return {"dtm_result": "SUCCESS"}


if __name__ == '__main__':
    app.run()
