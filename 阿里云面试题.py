# 用python语言, 写一个"老师"进程, 要求:

# 1. 进程启动时, 会往teacher表里面插入一条记录, 记下这条记录的id值, 保存为t_id.  ---ok

# 2. 进程定期更新其t_id对应记录的"check_time"字段. --实现

# 3. 进程定期扫描student表, 如果有teacher_id为0的记录, 将该字段更新为其t_id.

# 4. 当启动两个进程A和B时, 这两个进程可以平分所有的student, 即一半student的teacher_id为进程A的t_id,
#  另一半student的teacher_id为进程B的t_id。

# 5. 再启动一个进程C的时候, 1/3的student的teacher_id为进程A的t_id, 1/3为归进程B, 1/3归进程C.

# 6. 当进程A因为各种原因挂了, 不再更新check_time字段时, 进程B和进程C可以瓜分进程A托管的学生.


import multiprocessing
from pymysql import *
import datetime
import threading
import time
import os


class MyMysql(object):
    """
    进程类
    """

    def __init__(self):
        self.row_range = ""  # 扫面的工作范围元组类型
        self.con = connect(
            host='localhost',
            port=3306,
            database='ceshi',
            user='root',
            password='mysql',
            charset='utf8')
        self.cs = self.con.cursor()
        self.time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.t_id = ""
        self.q = ""  # todo 这里留一个通讯队列
        self.q1 = ""  # 线程运行数量队列

    def mysql_init(self):
        """
        # 1. 进程启动时, 会往teacher表里面插入一条记录, 记下这条记录的id值, 保存为t_id.
        :return: 返回值是t_id
        """
        self.cs.execute("""insert into teacher(check_time) \
                values(str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s'));""" % self.time)
        self.cs.execute("""SELECT LAST_INSERT_ID()""")
        self.t_id = self.cs.fetchone()[0]

    def update_teacher(self):
        """
        # 2. 进程定期更新其t_id对应记录的"check_time"字段. --实现
        :return:
        """
        while True:
            print("update ok")
            time.sleep(3)
            self.time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.cs.execute("""update teacher set \
                    check_time=str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s') where id=%d;""" % (self.time, self.t_id))
            self.con.commit()

    def up_stu_tid(self):
        """
        # 3. 进程定期扫描student表, 如果有teacher_id为0的记录, 将该字段更新为其t_id.
        # 4. 当启动两个进程A和B时, 这两个进程可以平分所有的student, 即一半student的teacher_id为进程A的t_id,
        #    另一半student的teacher_id为进程B的t_id。
        # 5. 再启动一个进程C的时候, 1/3的student的teacher_id为进程A的t_id, 1/3为归进程B, 1/3归进程C.

        :return:
        """
        while True:
            time.sleep(5)
            self.cs.execute(
                """select id from student where teacher_id=0 limit %d,%d;""" %
                self.row_range)
            a = self.cs.fetchall()
            for i in a:
                self.cs.execute(
                    """update student set teacher_id=%s where id= %s""" %
                    (self.t_id, i[0]))

    def tell_mian_live(self):
        """
        告诉主进程我还活着 和 进程 id
        此函数要定期执行
        :return:
        """
        time.sleep(1)  # 每两秒执行一次，进行函数的使用
        self.q.put(os.getpid())  # 将子进程号放到队列中

    def get_p_live(self):
        """
        更改全局变量
        :return:
        """
        while True:
            try:
                self.row_range = self.q1.get()
            except Exception as e:
                print(e)

    def my_pro_run(self, range_r, q, q1):
        """
        总的运行程序
        :return:
        """
        self.q = q  # 重置进程队列
        self.q1 = q1
        self.row_range = range_r
        print(self.row_range)
        self.mysql_init()
        print(self.t_id, type(self.t_id))
        # todo 开启定时报告存活 功能
        threading.Thread(target=self.tell_mian_live).start()
        threading.Thread(target=self.update_teacher).start()
        threading.Thread(target=self.up_stu_tid).start()
        self.con.commit()


class JinCheng(object):
    def __init__(self, fun, num):
        self.conn = connect(
            host='localhost',
            port=3306,
            database='ceshi',
            user='root',
            password='mysql',
            charset='utf8')
        self.cs2 = self.conn.cursor()
        self.cs2.execute("select count(*) from teacher;")
        self.mysql_row = self.cs2.fetchall()[0][0]  # 数据中的行数
        self.pro_list = [fun] * num
        self.pro_arg = ""
        self.q = multiprocessing.Queue()  # todo 进程通讯工具
        self.q1 = multiprocessing.Queue()  # todo 进程通讯工具
        self.chang_num_pro = 0

    def start_pro(self):
        for i in self.pro_list:
            time.sleep(0.02)  # 延迟0.02 秒 生成文件
            multiprocessing.Process(target=i, args=(
                (self.pro_arg[0][len(self.pro_list) - 1], self.pro_arg[1]), self.q, self.q1)
                                    ).start()
            print(" 开启成功")

    def cha_xu(self, a, b):
        """
        :param a: 数据库行数
        :param b: 进程数
        :return: 步进值和点值 -》 第几个进程 号-1  加步长
        """
        step_int = a // len(self.pro_list)
        if step_int % 2 == 0:
            step = round(a / b) + 1
        else:
            step = round(a / b)
        limit_page = []
        for i in range(0, a, step):
            limit_page.append(i)

        self.pro_arg = (limit_page, step)
        print(self.pro_arg)

    def check_pro(self):
        """
        判断开启的进程和进程列表中的数量是不是相同
        此函数单独开工线程使用
        :return:
        """
        start_pro = len(self.pro_list)
        print("start_pro", start_pro)

        def q_getpid(start_pro):
            get_q = []  # 进程号列表
            for i in range(start_pro):
                try:
                    count = self.q.get()
                    if count in get_q:
                        pass
                    else:
                        get_q.append(count)
                except Exception as e:
                    print(e)
            return get_q
        while True:
            a = q_getpid(start_pro)
            b = q_getpid(start_pro)
            if a == b:
                pass
            else:
                for i in a:
                    if i in b:
                        pass
                    else:
                        print("死掉的进程", i)
                        start_pro -= 1  # 进程数减少1
                        self.chang_num_pro = start_pro
                        self.cha_xu(self.mysql_row, self.chang_num_pro)
                        self.q1.put(self.pro_arg)  # 将更改后的参数发送给子进程

    def run_pro(self):
        """
        运行初始化
        :return:
        """

        self.cha_xu(self.mysql_row, len(self.pro_list))
        self.start_pro()
        threading.Thread(target=self.check_pro).start()


if __name__ == '__main__':

    a = MyMysql()
    jin = JinCheng(a.my_pro_run, 3)
    jin.run_pro()
