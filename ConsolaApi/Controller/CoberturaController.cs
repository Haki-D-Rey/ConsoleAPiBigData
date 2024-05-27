using ConsoleApi.Model;
using Microsoft.AspNetCore.Mvc;
using Microsoft.SqlServer.Server;
using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Net;
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
            { "2", "F" },
            { "1", "M" }
        };

        public static Dictionary<string, int> KeyFilterIndex { get; } = new Dictionary<string, int>
        {
            { "ORD", 2 },
            { "CARGA_I", 13 },
            { "ORIGEN", 14 },
            { "DEFAULT", 00 }
        };

        public static Dictionary<string, string> KeyFilterClasTipoU { get; } = new Dictionary<string, string>
        {
            { "00", "TITULAR" },
            { "1", "CONYUGE" },
            { "2", "MADRE" },
            { "3", "PADRE" },
            { "4", "HIJO" }
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
            // Devolver una respuesta exitosa (código 200)
            List<dynamic> respuestas = new List<dynamic>();
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

                    respuestas.Add(new { StatusCode = 200, Message = "Bulk insert desde Access a SQL Server completado exitosamente.", Response = new { /* datos */ } });
                    return Ok(respuestas);
                }
                catch (Exception ex)
                {
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

                string[] archivosCSV = Directory.GetFiles(rutaDirectorio, "Cobertura - FULL.csv");
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
                            { "Fin_Cob", "ID"},
                            { "Publico", "ORIGEN_1" },
                            { "Mes_Proceso", "ORD" },
                            { "Identidad", "IDENTIDAD" },
                            { "P_nombre", "NOMBRE1" },
                            { "S_nombre", "NOMBRE2" },
                            { "P_Apellido", "APELLIDO1" },
                            { "S_apellido", "APELLIDO2" },
                            { "Sexo", "SEXO" },
                            { "Fecha_Nac", "F_NAC" },
                            { "Dgrado", "GRADO" },
                            { "Fecha_ing", "F_ING" },
                            { "Clas_usuario", "TIPO" },
                            { "LugarChequeo", "CARGA_I" },
                            { "Estado", "ORIGEN" },
                            { "Dunidad", "UM" },
                            { "Dcargo", "CARGO" },
                            { "Expediente", "N_EXPEDIENTE" },
                            { "Tipo_regiStro", "CLASIFICACION" },
                            { "Cedula", "CEDULA" },
                            { "NCarnet", "NO_CARNE" },
                        };


                        // Agregar las columnas en el orden correcto
                        foreach (var entry in columnMapping)
                        {
                            string columnNameInDB = entry.Value;

                            // Determinar el tipo de datos de la columna
                            Type columnType = (columnNameInDB == "F_NAC" || columnNameInDB == "F_ING") ? typeof(DateTime) : ((columnNameInDB == "ID" || columnNameInDB == "ORD") ? typeof(int) : typeof(string));

                            // Agregar la columna con el nombre correspondiente de la base de datos y el tipo de datos determinado
                            dataTable.Columns.Add(columnNameInDB, columnType);
                        }

                        // Leer y agregar los datos al nuevo DataTable, empezando desde la segunda fila
                        bool primeraFila = true;
                        int numColumnas = columnMapping.Count;
                        List<(string key, int index)> columnasOrdenadas = new List<(string key, int index)>();
                        string[] columnasCSV = [];
                        int[] indicesIncluidos = [];
                        int contador = 0;
                        while (!lector.EndOfStream)
                        {

                            string linea = lector.ReadLine();

                            // Saltar la primera fila que contiene los nombres de las columnas
                            if (primeraFila)
                            {
                                // Dividir la línea para obtener las columnas
                                //columnasCSV = linea.Split(';');

                                columnasCSV = linea.Split(new char[] { ',', ';' });

                                // Arreglo de campos a excluir
                                string[] camposExcluidos = { "Fecha_Ret", "Inico_Cob", "Grado" };

                                // Filtrar el arreglo original excluyendo los campos especificados
                                string[] columnasFiltradas = columnasCSV.Except(camposExcluidos).ToArray();

                                // Sacar los índices de los campos incluidos
                                indicesIncluidos = Enumerable.Range(0, columnasCSV.Length)
                                                                    .Where(i => !camposExcluidos.Contains(columnasCSV[i]))
                                                                    .ToArray();


                                // Iterar sobre el arreglo de columnas CSV
                                for (int i = 0; i < columnasFiltradas.Length; i++)
                                {
                                    string columna = columnasFiltradas[i];
                                    if (columnMapping.ContainsKey(columna))
                                    {
                                        string mappedKey = columnMapping[columna];
                                        int index = Array.FindIndex(columnasFiltradas, key => columnMapping[key] == mappedKey);
                                        columnasOrdenadas.Add((mappedKey, index));
                                    }
                                }

                                primeraFila = false;
                                continue;
                            }

                            string[] valores = linea.Split(new char[] { ',', ';' });

                            // Filtrar el arreglo de valores excluyendo los valores correspondientes a los campos excluidos
                            string[] valoresFiltrados = valores.Select((val, i) => indicesIncluidos.Contains(i) ? val : null).ToArray();

                            // Crear un nuevo array para almacenar los valores en el orden correcto
                            object[] valoresOrdenados = new object[numColumnas];

                            // También podrías eliminar los valores nulos del arreglo si lo deseas.
                            valores = valoresFiltrados.Where(val => val != null).ToArray();

                            // Declara la variable i fuera del ciclo interno
                            int j = 0;
                            int k = 0;
                            // Itera sobre el diccionario de mapeo de columnas
                            foreach (var kvp in columnasOrdenadas)
                            {
                                j = 0;
                                // Verifica si el valor de la entrada coincide con alguna de las claves del resultado
                                foreach (var item in columnMapping)
                                {
                                    if (item.Value == kvp.key)
                                    {
                                        KeyFilterIndex.TryGetValue(kvp.key, out int valueindex);
                                        int index;

                                        if (!KeyFilterIndex.TryGetValue(kvp.key, out valueindex))
                                        {
                                            index = j; // Si no se encuentra kvp.key en el diccionario, se asigna el valor de j a index
                                        }
                                        else
                                        {
                                            index = valueindex; // Si se encuentra kvp.key en el diccionario, se asigna el valor asociado a index
                                        }

                                        if (kvp.key == "ID" || kvp.key == "ORD")
                                        {
                                            valoresOrdenados[index] = contador;
                                            break;
                                        }
                                        // Si hay coincidencia, agregar el valor al resultado
                                        valoresOrdenados[index] = ValidacionesValores(valores, kvp.index, kvp.key);
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

                            contador++;
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
                    KeyFilterClas.TryGetValue(valor[index], out string KeyValue);
                    resultado = KeyValue;
                }

                if (nameColumn == "TIPO")
                {
                    KeyFilterClasTipoU.TryGetValue(valor[index], out string KeyValue);
                    resultado = KeyValue;
                }

                if (nameColumn == "SEXO")
                { 
                    KeyFilterSex.TryGetValue(valor[index], out string KeyValue);
                    resultado = KeyValue;
                }

                if (nameColumn == "ORIGEN_1" || nameColumn == "ORIGEN") {
                    resultado = "AUTOMATICO";
                }

                if (nameColumn == "CARGA_I") {
                    resultado = "t";
                }

                if (nameColumn == "GRADO") {
                    resultado = valor[index];
                    //List<Grado> ListaGradoMilitar = (ObtenerGradosMilitares(valor[index]));
                    //resultado = ListaGradoMilitar.First().DESCRIPCION;
                }


                if (nameColumn == "CEDULA")
                {
                    resultado = valor[index];
                    //List<Grado> ListaGradoMilitar = (ObtenerGradosMilitares(valor[index]));
                    //resultado = ListaGradoMilitar.First().DESCRIPCION;
                }

                if (nameColumn == "F_ING" || nameColumn == "F_NAC")
                {
                    string formato = "dd/M/yyyy";
                    if (DateTime.TryParseExact(valor[index], formato, null, System.Globalization.DateTimeStyles.None, out DateTime fecha))
                    {

                        if (fecha < new DateTime(1900, 1, 1))
                        {
                            // La fecha es menor que 01/01/1900, asignar un valor predeterminado de "01/01/1900"
                            resultado = "01/01/1900 00:00:00";
                        }
                        else
                        {
                            resultado = fecha.ToString("dd/MM/yyyy"); // Asignar la fecha al arreglo
                        }
                    }
                    string formato2 = "dd/MM/yyyy";
                    if (DateTime.TryParseExact(valor[index], formato2, null, System.Globalization.DateTimeStyles.None, out DateTime fecha2))
                    {

                        if (fecha2 < new DateTime(1900, 1, 1))
                        {
                            // La fecha es menor que 01/01/1900, asignar un valor predeterminado de "01/01/1900"
                            resultado = "01/01/1900 00:00:00";
                        }
                        else
                        {
                            resultado = fecha.ToString("dd/MM/yyyy"); // Asignar la fecha al arreglo
                        }
                    }

                    string formato5 = "d/M/yyyy";
                    if (DateTime.TryParseExact(valor[index], formato5, null, System.Globalization.DateTimeStyles.None, out DateTime fecha5))
                    {

                        if (fecha5 < new DateTime(1900, 1, 1))
                        {
                            // La fecha es menor que 01/01/1900, asignar un valor predeterminado de "01/01/1900"
                            resultado = "01/01/1900 00:00:00";
                        }
                        else
                        {
                            resultado = fecha5.ToString("dd/MM/yyyy"); // Asignar la fecha al arreglo
                        }
                    }

                }
            } 
            catch(Exception ex) {
                // Handle any exceptions here
                resultado = "";
                Console.WriteLine("Error al validar columnas: " + ex.Message, valor);
            }
            return resultado;
        }
    }
}
