using System;

public class Persona
{
    private DateTime? f_Nac;
    private DateTime? f_Ing;

    public int ID { get; set; }
    public string ORIGEN_1 { get; set; }
    public int ORD { get; set; }
    public string IDENTIDAD { get; set; }
    public string NOMBRE1 { get; set; }
    public string NOMBRE2 { get; set; }
    public string APELLIDO1 { get; set; }
    public string APELLIDO2 { get; set; }
    public string SEXO { get; set; }
    public DateTime? F_NAC
    {
        get { return f_Nac; }
        set { f_Nac = value ?? new DateTime(1990, 1, 1); }
    }
    public string GRADO { get; set; }
    public DateTime? F_ING
    {
        get { return f_Ing; }
        set { f_Ing = value ?? new DateTime(1990, 1, 1); }
    }
    public string TIPO { get; set; }
    public string CARGA_I { get; set; }
    public string ORIGEN { get; set; }
    public string UM { get; set; }
    public string CARGO { get; set; }
    public string N_EXPEDIENTE { get; set; }
    public string CLASIFICACION { get; set; }
    public string CEDULA { get; set; }
    public string NO_CARNE { get; set; }
    public DateTime fecha_ingreso_BD { get; set; }

    public void CleanUp()
    {
        ORIGEN_1 = ORIGEN_1?.Trim();
        IDENTIDAD = IDENTIDAD?.Trim();
        NOMBRE1 = NOMBRE1?.Trim();
        NOMBRE2 = NOMBRE2?.Trim();
        APELLIDO1 = APELLIDO1?.Trim();
        APELLIDO2 = APELLIDO2?.Trim();
        SEXO = SEXO?.Trim();
        GRADO = GRADO?.Trim();
        TIPO = TIPO?.Trim();
        CARGA_I = CARGA_I?.Trim();
        ORIGEN = ORIGEN?.Trim();
        UM = UM?.Trim();
        CARGO = CARGO?.Trim();
        N_EXPEDIENTE = N_EXPEDIENTE?.Trim();
        CLASIFICACION = CLASIFICACION?.Trim();
        CEDULA = CEDULA?.Trim();
        NO_CARNE = NO_CARNE?.Trim();
    }
}
