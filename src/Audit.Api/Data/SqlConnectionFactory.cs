using Microsoft.Data.SqlClient;

namespace Audit.Api.Data;

public sealed class SqlConnectionFactory(string connectionString)
{
    public SqlConnection Create() => new(connectionString);
}
