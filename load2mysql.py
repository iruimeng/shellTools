class Hive2mysqlCls(MysqlCls):
    """
    exec hql and load log 2 mysql
    """
    def __init__(self, mq):
        super(Hive2mysqlCls, self).__init__(mq)
        #匹配log日志记录
        self.hiveSed = "sed -n '/Moved to trash:/,/Time taken:/p' <file> | sed '/Moved to trash:/d' | sed -e '1,1d' | sed -e '$ d'"
        

    def run(self, d):
        if not isinstance(d, dict):
            return False
        #super(Hive2mysqlCls, self).run(d)
        log = self.logFile(d.get('task_key'), d.get('task_day'))
        rs = self.cmdRun(self.pcmd(d.get('cmd')), log, log)
        if rs == 0:
            self.__loadFile2Mysql(d, log)

        return self.taskGc(rs, d, 'hive')




    def __loadFile2Mysql(self, d, log):
        """
        #导Log日志到msyql数据库
        """
        #ctime = ctime.strftime("%Y%m%d")
        #log = os.path.join(historyModel().logPath, ctime, key)

        #sed过滤出hive log数据
        str = subprocess.check_output(self.hiveSed.replace('<file>', log), shell=True)
        if not str.strip():
            return False

        db, table, field = self.__parseOut(d.get('out_values'))
        
        if table == False or field == False:
            self.log.error("task out_values set error, task:%s" % str(d))
            return False

        cleanList = []
        for line in str.split("\n"):
            if not line:
                continue
            ll = line.split("\t")
            if len(ll) != len(field):
                self.log.info('colums num not match: %s **** %s' % (line, key))
                continue
            #进行字符串的urldecode
            map(urllib.unquote, ll)
            ll.insert(0, date)
            cleanList.append(ll)


        if len(cleanList) == 0:
            self.log.info('result is empty: %s' % key)
            return False

        db = self.conn()
        if not db:
            self.log.error("db connection error")
            return False

        sql = self.__makeInsertSql(table, field)
        #入库mysql逻辑，分批次入库。按步长分片
        for z in range(len(cleanList)):
            cleanList[z] = map(lambda x: self.__iif(x == 'NULL', '', x), cleanList[z])

            stepNum = 10
            for ii in xrange(0, len(cleanList), stepNum):
                l = cleanList[ii:ii+stepNum]
                if l:
                    try:
                        rs = db.insertmany(sql, l)
                    except Exception as e:
                        self.log.error(traceback.format_exc())
                else:
                    continue
        return True

    def __parseOut(self, outval):
        """
        #解析配置
        db.table:field1, field2
        """
        if outval.find(':') == -1:
            self.log.error("task out_values set error, task:%s" % outval)
            return (False, False, False)
        ov = outval.split(':')

        field = ov[1].split(',')
        map(lambda x: x.strip(), field)
        
        if ov[0].find('.') == -1:
            self.log.error("task out_values db.table set error, task:%s" % outval)
            return (False, False, False)
            
        dt = ov[0].split('.')
        return (dt[0], dt[1], field)



    def __makeInsertSql(self, table, outList):
        """
        #生成入库sql
        """
        l = []
        for i in outList:
            l.append("%s=VALUES(%s)" % (i, i))
        k = ','.join(l) 
        f = "ctime, `%s`" % "`,`".join(outList) 
        v = ("%s," * (len(outList) + 1))[:-1]
        sql = "insert into %s (%s) values (%s) ON DUPLICATE KEY UPDATE %s" % (table, f, v, k)
        return sql

