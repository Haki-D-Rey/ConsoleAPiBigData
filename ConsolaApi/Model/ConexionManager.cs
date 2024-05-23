using System;
using System.Data.SqlClient;

namespace ConsoleApi.Model
    {
        public class ConexionManager : IDisposable
        {
            private readonly string _connectionString;
            private SqlConnection _connection;

            public ConexionManager(string connectionString)
            {
                _connectionString = connectionString;
            }

            public void OpenConnection()
            {
                try
                {
                    _connection = new SqlConnection(_connectionString);
                    _connection.Open();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error al abrir la conexión SQL Server: " + ex.Message);
                    throw; // Propaga la excepción para manejarla en el nivel superior si es necesario
                }
            }

            public SqlConnection GetConnection()
            {
                if (_connection == null || _connection.State != System.Data.ConnectionState.Open)
                {
                    OpenConnection();
                }
                return _connection;
            }

            public void CloseConnection()
            {
                if (_connection != null && _connection.State == System.Data.ConnectionState.Open)
                {
                    _connection.Close();
                    Console.WriteLine("Conexión SQL Server cerrada correctamente.");
                }
            }

            public void Dispose()
            {
                if (_connection != null)
                {
                    _connection.Dispose();
                    _connection = null;
                }
            }
        }
    }