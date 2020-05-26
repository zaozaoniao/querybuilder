using System;
using System.Collections.Generic;
using SqlKata;
using SqlKata.Compilers;
using SqlKata.Execution;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Linq;
using Newtonsoft.Json;
using Npgsql;
using System.Data;
using Dapper;
using System.Data.SQLite;
using static SqlKata.Expressions;
using System.IO;
using System.Data.Odbc;
using Gridsum.Common.QueryIntegration;

namespace Program
{
    class Program
    {
        private class Loan
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public List<Installment> Installments { get; set; } = new List<Installment>();
        }

        private class Installment
        {
            public string Id { get; set; }
            public string LoanId { get; set; }
            public int DaysCount { get; set; }
        }

        static void Main(string[] args)
        {
          //  ODBCQueryFactory1();
            //ODBCQueryFactory2();
            //ODBCQueryFactory3();
            ODBCQueryFactory4();
            //var query = new Query("accounts").AsInsert(new
            //{
            //    name = "new Account",
            //    currency_id = "USD",
            //    created_at = DateTime.UtcNow,
            //    Value = SqlKata.Expressions.UnsafeLiteral("nextval('hello')", replaceQuotes: false)
            //});

            //var compiler = new SqlServerCompiler();
            //var sql = compiler.Compile(query).Sql;
            //Console.WriteLine(sql);
        }

        private static void log(Compiler compiler, Query query)
        {
            var compiled = compiler.Compile(query);
            Console.WriteLine(compiled.ToString());
            Console.WriteLine(JsonConvert.SerializeObject(compiled.Bindings));
        }

        private static QueryFactory SqlLiteQueryFactory()
        {
            var compiler = new SqliteCompiler();

            var connection = new SQLiteConnection("Data Source=Demo.db");

            var db = new QueryFactory(connection, compiler);

            db.Logger = result =>
            {
                Console.WriteLine(result.ToString());
            };

            if (!File.Exists("Demo.db"))
            {
                Console.WriteLine("db not exists creating db");

                SQLiteConnection.CreateFile("Demo.db");

                db.Statement("create table accounts(id integer primary key autoincrement, name varchar, currency_id varchar, created_at datetime);");

            }

            return db;
        }

        /// <summary>
        ///  基础过滤，聚合，条件判断sql生成
        /// </summary>
        /// <returns></returns>
        private static QueryFactory ODBCQueryFactory1()
        {
            var compiler = new GenericCompiler();
            var connection = new OdbcConnection(
                "Driver=Cloudera ODBC Driver for Impala;Host=10.201.82.170;Port=21050;Schema=eap_1"
            );
            var db = new QueryFactory(connection, compiler);

            // CommonQuery传递的前端参数转换为 sql字段
            var queryDto = new CommonQuery
            {
                Columns = new List<string> { "ss", "ss" },
                GroupBys = new List<string> { "s_event_name" },

            };

            var query = db.Query("event_today").Where("s_event_name", "s_app_launch").OrWhere("s_event_name", "s_app_show").GroupBy("s_event_name")
                .Select("s_event_name", "count(*) as CC");                       // .OrWhere("s_event_name", "s_page_show").
            var sql = compiler.Compile(query);

            // var res = db.Statement(sql.ToString());   // 这玩意返回的是受影响的行数，select 始终返回-1，我们不会更新数据库，所以这个方法我们不用。
            var result = query.Get<ResultEntity>();

            foreach( var s  in result)
            {
                var ssss = s;

            }

            //var dr = new DataResult
            //{
            //    Headers = result.
            //};

            db.Logger = rs =>
            {
                Console.WriteLine(rs.ToString());
            };
            return db;
        }

        /// <summary>
        /// 直接传递sql,返回rawdata
        /// </summary>
        /// <returns></returns>
        private static QueryFactory ODBCQueryFactory2()
        {
            var compiler = new GenericCompiler();
            var connection = new OdbcConnection(
                "Driver=Cloudera ODBC Driver for Impala;Host=10.201.82.170;Port=21050;Schema=eap_1"
            );

            var db = new QueryFactory(connection, compiler);

            var result = db.Select("select s_event_name,count(*)  as cx from event_today where  s_event_name=? group by s_event_name",
                 new Dictionary<string, object> { { "?0", "s_app_launch" }, }
                );
            db.Logger = rs =>
            {
                Console.WriteLine(rs.ToString());
            };
            return db;
        }

        /// <summary>
        /// 直接传递sql, 并将返回结构强类型
        /// </summary>
        /// <returns></returns>
        private static QueryFactory ODBCQueryFactory3()
        {
            var compiler = new GenericCompiler();
            var connection = new OdbcConnection(
                "Driver=Cloudera ODBC Driver for Impala;Host=10.201.82.170;Port=21050;Schema=eap_1"
            );

            var db = new QueryFactory(connection, compiler);

            var result = db.Select<ResultEntity>("select s_event_name,count(*)  as cx from event_today where  s_event_name=?   or  s_event_name=?  group by s_event_name",
                 new Dictionary<string, object> {
                     { "?0", "s_app_launch" },
                     { "?1","s_page_show" } }
                );

            db.Logger = rs =>
            {
                Console.WriteLine(rs.ToString());
            };
            return db;
        }

        /// <summary>
        /// 支持CTE
        /// </summary>
        /// <returns></returns>
        private static QueryFactory ODBCQueryFactory4()
        {
            var compiler = new GenericCompiler();
            var connection = new OdbcConnection(
                "Driver=Cloudera ODBC Driver for Impala;Host=10.201.82.170;Port=21050;Schema=eap_1"
            );

            var db = new QueryFactory(connection, compiler);

            var init_event = new Query("event_today")
                 .Select("s_final_user_id", "s_brand", "s_day")
                 .Where("s_event_name", "s_app_launch").WhereBetween("s_day",20200520,20200526);

            var second_event = new Query("event_today")
                .With("init_event", init_event) // now you can consider ActivePosts as a regular table in the database
                .Join("init_event", "init_event.s_final_user_id", "event_today.s_final_user_id")
                .Where("s_event_name", "s_app_show")
                .GroupByRaw("s_final_user_id,intervalDays,s_brand")
                .SelectRaw("event_today.s_final_user_id,event_today.s_brand,(event_today.s_day-init_event.s_day) as intervalDays");

            var query = db.Query("second_event")
                .With("second_event", second_event)
                .GroupByRaw("s_brand,intervalDays")
                .SelectRaw("s_brand,intervalDays,count( DISTINCT s_final_user_id),count(*) as  times,count (*) over()  as totalCount ");

            var sql = compiler.Compile(query);

            var ss = db.Select(sql.ToString());
            var result = query.Get();

            db.Logger = rs =>
            {
                Console.WriteLine(rs.ToString());
            };
            return db;
        }

        private static QueryFactory SqlServerQueryFactory()
        {
            var compiler = new PostgresCompiler();
            var connection = new SqlConnection(
               "Server=tcp:localhost,1433;Initial Catalog=Lite;User ID=sa;Password=P@ssw0rd"
           );

            var db = new QueryFactory(connection, compiler);

            db.Logger = result =>
            {
                Console.WriteLine(result.ToString());
            };

            return db;
        }

    }
}
