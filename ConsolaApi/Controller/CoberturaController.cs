using ConsoleApi.Model;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Reflection;


namespace MyRestApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CoberturaController : ControllerBase
    {
        private static readonly string connectionString = "Data Source=10.0.29.7;Initial Catalog=Prueba-VDHMIL;User ID=sa;Password=&ecurity23;";
        private static readonly string connectionStringProd = "Data Source=HMIL-GRM-APPPRD;Initial Catalog=DPC;User ID=sa;Password=P@$$W0RD;";

        private static SqlConnection sqlConnection = null;
        private static string rutaDirectorio = @"C:/Users/cesar.cuadra/Downloads/";//@"/home/haki/csv/"; // Ruta donde están tus archivos CSV

        // Propiedad pública para el diccionario de mapeo de columnas
        public static Dictionary<string, string> KeyFilterClas { get; } = new Dictionary<string, string>
        {
            { "P", "PENSIONADO" },
            { "C", "CBI" },
            { "F", "FALLECIDO" },
            { "R", "ROUM" },
            { "A", "ACTIVO" }
        };

        public static Dictionary<string, string> KeyFilterSex { get; } = new Dictionary<string, string>
        {
            { "1", "M" },
            { "2", "F" }
        };

        public static Dictionary<string, string> KeyFilterClasTipoU { get; } = new Dictionary<string, string>
        {
            { "00", "TITULAR" },
            { "01", "CONYUGE" },
            { "02", "MADRE" },
            { "03", "PADRE" },
            { "04", "HIJO" }
        };

        public CoberturaController()
        {
            sqlConnection = new SqlConnection(connectionString);
        }

        // GET: api/<cobertura>
        [HttpGet]
        public IEnumerable<dynamic> Get()
        {
            var connection2 = new ConexionManager(connectionStringProd).GetConnection();    
            using (var connection = new ConexionManager(connectionString).GetConnection())
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText = "SELECT * FROM dbo.[MAESTRO DE DPC]"; // Remove "@" before "SELECT"
                var dataReader = cmd.ExecuteReader();
                List<Persona> ListaPersonas = new List<Persona>();

                if (dataReader.HasRows)
                {
                    while (dataReader.Read())
                    {
                        Persona persona = new Persona
                        {
                            ID = dataReader.GetInt32(dataReader.GetOrdinal("ID")),
                            ORIGEN_1 = dataReader.GetString(dataReader.GetOrdinal("ORIGEN_1")),
                            ORD = dataReader.GetInt32(dataReader.GetOrdinal("ORD")),
                            IDENTIDAD = dataReader.GetString(dataReader.GetOrdinal("IDENTIDAD")),
                            NOMBRE1 = dataReader.GetString(dataReader.GetOrdinal("NOMBRE1")),
                            NOMBRE2 = dataReader.GetString(dataReader.GetOrdinal("NOMBRE2")),
                            APELLIDO1 = dataReader.GetString(dataReader.GetOrdinal("APELLIDO1")),
                            APELLIDO2 = dataReader.GetString(dataReader.GetOrdinal("APELLIDO2")),
                            SEXO = dataReader.GetString(dataReader.GetOrdinal("SEXO")),
                            F_NAC = dataReader.IsDBNull(dataReader.GetOrdinal("F_NAC")) ? (DateTime?)null : dataReader.GetDateTime(dataReader.GetOrdinal("F_NAC")),
                            GRADO = dataReader.GetString(dataReader.GetOrdinal("GRADO")),
                            F_ING = dataReader.IsDBNull(dataReader.GetOrdinal("F_ING")) ? (DateTime?)null : dataReader.GetDateTime(dataReader.GetOrdinal("F_ING")),
                            TIPO = dataReader.GetString(dataReader.GetOrdinal("TIPO")),
                            CARGA_I = dataReader.GetString(dataReader.GetOrdinal("CARGA_I")),
                            ORIGEN = dataReader.GetString(dataReader.GetOrdinal("ORIGEN")),
                            UM = dataReader.GetString(dataReader.GetOrdinal("UM")),
                            CARGO = dataReader.GetString(dataReader.GetOrdinal("CARGO")),
                            N_EXPEDIENTE = dataReader.GetString(dataReader.GetOrdinal("N_EXPEDIENTE")),
                            CLASIFICACION = dataReader.GetString(dataReader.GetOrdinal("CLASIFICACION")),
                            CEDULA = dataReader.GetString(dataReader.GetOrdinal("CEDULA")),
                            NO_CARNE = dataReader.GetString(dataReader.GetOrdinal("NO_CARNE")),
                            fecha_ingreso_BD = dataReader.GetDateTime(dataReader.GetOrdinal("fecha_ingreso_BD"))
                        };
                        ListaPersonas.Add(persona);
                    }
                }

                return ListaPersonas;
            }
        }

        // POST: api/<cobertura>
        [HttpPost]
        public IActionResult BulkInsertFromAccess()
        {

            var connection = new ConexionManager(connectionString).GetConnection();

            if (connection != null)
            {
                try
                {
                    Stopwatch totalStopwatch = Stopwatch.StartNew(); // Iniciar el cronómetro total

                    ProcesarArchivosCSV(rutaDirectorio);

                    totalStopwatch.Stop(); // Detener el cronómetro total
                    Console.WriteLine($"Tiempo total de procesamiento: {totalStopwatch.Elapsed}");

                    connection.Close();

                    // Devolver una respuesta exitosa (código 200)
                    return Ok("Bulk insert desde Access a SQL Server completado exitosamente.");
                }
                catch (Exception ex)
                {
                    // En caso de error, devolver un código de error (por ejemplo, 500) con el mensaje de error
                    return StatusCode(500, $"Error: {ex.Message}");
                }
            }
            else
            {
                Console.WriteLine("No se pudo abrir la conexión con la base de datos.");

                // En caso de que la conexión falle, también devolver un código de error (por ejemplo, 500)
                return StatusCode(500, "Error al abrir la conexión con la base de datos.");
            }
        }

        public void ProcesarArchivosCSV(string rutaDirectorio)
        {
            try
            {
                string[] archivosCSV = Directory.GetFiles(rutaDirectorio, "*.csv");
                foreach (string archivo in archivosCSV)
                {
                    Stopwatch archivoStopwatch = Stopwatch.StartNew(); // Iniciar el cronómetro para el archivo actual

                    using (var lector = new StreamReader(archivo))
                    {
                        // Crear un DataTable para almacenar los datos
                        DataTable dataTable = new DataTable();

                        // Define el diccionario de mapeo de columnas
                        Dictionary<string, string> columnMapping = new Dictionary<string, string>()
                        {
                            { "Tipo_regiStro", "ORIGEN_1" },
                            { "Ord", "ORD" },
                            { "Identidad", "IDENTIDAD" },
                            { "P_nombre", "NOMBRE1" },
                            { "S_nombre", "NOMBRE2" },
                            { "P_Apellido", "APELLIDO1" },
                            { "S_apellido", "APELLIDO2" },
                            { "Sexo", "SEXO" },
                            { "Fecha_Nac", "F_NAC" },
                            { "Grado", "GRADO" },
                            { "Fecha_ing", "F_ING" },
                            { "Clas_usuario", "TIPO" },
                            { "Carga_I", "CARGA_I" },
                            { "Expediente", "N_EXPEDIENTE" },
                            { "Tipo_regiStro2", "ORIGEN" },
                            { "Dunidad", "UM" },
                            { "Dcargo", "CARGO" },
                            { "Clasificacion", "CLASIFICACION" },
                            { "Cedula", "CEDULA" },
                            { "NCarnet", "NO_CARNE" },
                        };


                        // Agregar las columnas en el orden correcto
                        foreach (var entry in columnMapping)
                        {
                            string columnNameInDB = entry.Value;

                            // Agregar la columna con el nombre correspondiente de la base de datos
                            dataTable.Columns.Add(columnNameInDB);
                        }

                        List<Grado> ListaGradosMilitares = ObtenerGradosMilitares("S1");

                        // Leer y agregar los datos al nuevo DataTable, empezando desde la segunda fila
                        bool primeraFila = true;
                        int numColumnas = columnMapping.Count;
                        List<(string key, int index)> columnasOrdenadas = new List<(string key, int index)>();
                        //string[] columnasOrdenadas = new string[numColumnas]; // Arreglo para almacenar las columnas en orden
                        while (!lector.EndOfStream)
                        {
                            string linea = lector.ReadLine();
                            
                            // Saltar la primera fila que contiene los nombres de las columnas
                            if (primeraFila)
                            {
                                // Dividir la línea para obtener las columnas
                                string[] columnasCSV = linea.Split(',');

                                // Nuevo diccionario para almacenar el resultado
                                

                                // Iterar sobre el arreglo de columnas CSV
                                for (int i = 0; i < columnasCSV.Length; i++)
                                {
                                    string columna = columnasCSV[i];
                                    if (columnMapping.ContainsKey(columna))
                                    {
                                        string mappedKey = columnMapping[columna];
                                        int index = Array.FindIndex(columnMapping.Keys.ToArray(), key => columnMapping[key] == mappedKey);
                                        columnasOrdenadas.Add((mappedKey, index));
                                    }
                                }

                                foreach (var item in columnasOrdenadas)
                                {
                                    Console.WriteLine($"Key: {item.key}, Index: {item.index}");
                                }

                                // Reordenar las columnas según el mapeo y almacenarlas en el arreglo
                                //for (int i = 0; i < numColumnas; i++)
                                //{
                                //    string columnNameInCSV = columnMapping.ElementAt(i).Key;
                                //    string columnNameInBD = columnMapping.ElementAt(i).Value;
                                //    int index = Array.IndexOf(columnasCSV, columnNameInCSV);
                                //    if (index != -1)
                                //    {
                                //        index= columnNameInCSV == "Fecha_Nac" ? 12 : index;
                                //        index = columnNameInCSV == "Fecha_ing" ? 13 : index;
                                //        index = columnNameInCSV == "Cedula" ? 18 : index;
                                //        index = columnNameInCSV == "NCarnet" ? 19 : index;
                                //        columnasOrdenadas[index] = columnNameInBD;
                                //    }
                                //    else
                                //    {
                                //        // Si la columna no se encuentra en el archivo CSV, establecer un valor predeterminado
                                //        columnasOrdenadas[i] = columnNameInBD; // O cualquier otro valor predeterminado que desees
                                //    }
                                //}

                                primeraFila = false;
                                continue;
                            }

                            string[] valores = linea.Split(','); // Separar por comas

                            // Crear un nuevo array para almacenar los valores en el orden correcto
                            object[] valoresOrdenados = new object[numColumnas];
                            //List<Grado> ListaGradosMilitares = ObtenerGradosMilitares("S1");

                            // Mapear los valores de la fila actual al nuevo array según el arreglo de columnas ordenadas
                            //for (int i = 0; i < numColumnas; i++)
                            //{
                            //    if (!string.IsNullOrEmpty(columnasOrdenadas[i]))
                            //    {
                            //        int index = Array.IndexOf(columnasOrdenadas, columnasOrdenadas[i]);
                            //        valoresOrdenados[index] = ValidacionesValores(valores,index, columnasOrdenadas[i]);
                            //    }
                            //}

                            // Declara la variable i fuera del ciclo interno
                            int j = 0;
                            int k = 0;
                            // Itera sobre el diccionario de mapeo de columnas
                            foreach (var kvp in columnasOrdenadas)
                            {
                                // Verifica si el valor de la entrada coincide con alguna de las claves del resultado
                                foreach (var item in columnMapping)
                                {
                                    if (item.Value == kvp.key)
                                    {
                                        // Si hay coincidencia, agregar el valor al resultado
                                        valoresOrdenados[k] = ValidacionesValores(valores, kvp.index, kvp.key);
                                        break;
                                    }
                                    // Incrementa i solo si no hay coincidencia
                                    j++;
                                }
                                // Incrementa i aquí para avanzar al siguiente elemento en la próxima iteración
                                k++;
                            }

                            // Agregar la nueva fila al nuevo DataTable
                            dataTable.Rows.Add(valoresOrdenados);
                        }

                        using (SqlConnection sqlConnection = new SqlConnection(connectionString))
                        {
                            sqlConnection.Open();

                            // Iniciar la transacción para el bulk insert
                            SqlTransaction transaction = sqlConnection.BeginTransaction();

                            // Crear el objeto SqlBulkCopy
                            using (var bulkCopy = new SqlBulkCopy(sqlConnection, SqlBulkCopyOptions.Default, transaction))
                            {
                                bulkCopy.DestinationTableName = "dbo.[MAESTRO DE DPC]"; // Reemplaza con el nombre de tu tabla
                                bulkCopy.BatchSize = 10000; // Tamaño del lote a insertar
                                bulkCopy.BulkCopyTimeout = 600; // Tiempo de espera en segundos

                                try
                                {
                                    // Realizar el bulk insert
                                    bulkCopy.WriteToServer(dataTable);

                                    // Commit de la transacción si todo fue exitoso
                                    transaction.Commit();
                                    Console.WriteLine("Bulk insert exitoso para el archivo: " + archivo);
                                }
                                catch (Exception ex)
                                {
                                    // Rollback en caso de error
                                    Console.WriteLine("Error durante el bulk insert para el archivo " + archivo + ": " + ex.Message);
                                    transaction.Rollback();
                                }
                            }
                        }
                    }

                    // Pequeña pausa para permitir que el sistema libere el archivo
                    System.Threading.Thread.Sleep(100);

                    archivoStopwatch.Stop(); // Detener el cronómetro para el archivo actual
                    Console.WriteLine($"Tiempo de procesamiento para {archivo}: {archivoStopwatch.Elapsed}");
                }
            }
            catch (DirectoryNotFoundException)
            {
                Console.WriteLine($"Error: El directorio {rutaDirectorio} no existe");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
        }


        public List<Grado> ObtenerGradosMilitares(string codigo = null)
        {
            try
            {
                List<Grado> ListaGradosMilitares = new List<Grado>();
                using (var connection = new ConexionManager(connectionStringProd).GetConnection())
                {

                    var cmd = connection.CreateCommand();
                    cmd.CommandText = "SELECT * FROM dbo.[CAT DE GRADOS]"; // Remove "@" before "SELECT"
                    var dataReader = cmd.ExecuteReader();

                    if (dataReader.HasRows)
                    {
                        while (dataReader.Read())
                        {
                            Grado grado = new Grado
                            {
                                CODIGO = dataReader.GetString(dataReader.GetOrdinal("CODIGO")),
                                DESCRIPCION = dataReader.GetString(dataReader.GetOrdinal("DESCRIPCION"))
                            };
                            ListaGradosMilitares.Add(grado);
                        }
                    }
                }

                if (!string.IsNullOrEmpty(codigo))
                {
                    return ListaGradosMilitares.Where(grado => grado.CODIGO == codigo).ToList();
                }

                return ListaGradosMilitares;
            }
            catch (Exception ex)
            {
                // Handle any exceptions here
                Console.WriteLine("Error al obtener grados militares: " + ex.Message);
                // Return an empty array or null indicating failure
                return new List<Grado>();
            }
        }

        public dynamic ValidacionesValores(dynamic valor, int index ,string nameColumn ) {
            var resultado = "";
            try {
                resultado = valor[index] is string valorString ? valorString.Trim() : valor[index];

                if (nameColumn == "CLASIFICACION")
                {
                    KeyFilterClas.TryGetValue(valor[0], out string KeyValue);
                    resultado = KeyValue;
                }

                if (nameColumn == "TIPO")
                {
                    KeyFilterClasTipoU.TryGetValue(valor[11], out string KeyValue);
                    resultado = KeyValue;
                }

                if (nameColumn == "SEXO")
                {
                    KeyFilterClas.TryGetValue(valor[7], out string KeyValue);
                    resultado = KeyValue;
                }

                if (nameColumn == "ORIGEN_1" || nameColumn == "ORIGEN") {
                    resultado = "AUTOMATICO";
                }

                if (nameColumn == "CARGA_I") {
                    resultado = "t";
                }
            } 
            catch(Exception ex) {
                // Handle any exceptions here
                resultado = "";
                Console.WriteLine("Error al validar columnas: " + ex.Message);
            }
            return resultado;
        }
    }
}
