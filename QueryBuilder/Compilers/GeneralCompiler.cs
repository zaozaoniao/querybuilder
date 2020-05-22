using System.Collections.Generic;
using System.Linq;

namespace SqlKata.Compilers
{
    public class GenericCompiler : Compiler
    {
        public GenericCompiler()
        {
            OpeningIdentifier = ClosingIdentifier = " ";
            parameterPrefix = "?";
        }

        public override string EngineCode { get; } = EngineCodes.Generic;

        public override SqlResult Compile(Query query)
        {
            var ctx = CompileRaw(query);

            ctx = PrepareResult(ctx);

            return ctx;
        }

        protected new SqlResult PrepareResult(SqlResult ctx)
        {
            ctx.NamedBindings = generateNamedBindings(ctx.Bindings.ToArray());
            ctx.Sql = Helper.ReplaceAll(ctx.RawSql, parameterPlaceholder, i => parameterPrefix );  //  Command sql不能使用命名参数，只能使用？ 实际参数?0,?1 排列好
            return ctx;
        }

        protected new Dictionary<string, object> generateNamedBindings(object[] bindings)
        {
            return Helper.Flatten(bindings).Select((v, i) => new { i, v })
                .ToDictionary(x => parameterPrefix + x.i, x => x.v);
        }
    }
}

/*
 https://docs.microsoft.com/en-us/dotnet/api/system.data.odbc.odbccommand.parameters?redirectedfrom=MSDN&view=dotnet-plat-ext-3.1#System_Data_Odbc_OdbcCommand_Parameters

 https://stackoverflow.com/questions/32196416/select-query-does-not-work-with-parameters-using-parameters-addwithvalue
 
 */
