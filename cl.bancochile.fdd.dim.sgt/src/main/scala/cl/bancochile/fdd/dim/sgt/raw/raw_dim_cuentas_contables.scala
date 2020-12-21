package cl.bancochile.fdd.dim.sgt.raw 
         
//Project's global setting 
import cl.bancochile.fdd.todos.globalSettings._ 
 
import com.huemulsolutions.bigdata.common._ 
import com.huemulsolutions.bigdata.control._ 
import com.huemulsolutions.bigdata.datalake._ 
import com.huemulsolutions.bigdata.tables._ 
import org.apache.spark.sql.types._ 
import com.huemulsolutions.bigdata.control.huemulType_Frequency._ 
 
 
/** 
 * Clase que permite abrir un archivo de texto, devuelve un objeto huemul_dataLake con un DataFrame de los datos 
 * ejemplo de nombre: raw_institucion_mes 
 */ 
class raw_dim_cuentas_contables(huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_DataLake(huemulBigDataGov, Control) with Serializable  { 
   this.Description = "Cuentas_Contables" 
   this.GroupName = ("cmf").toUpperCase() 
    
   //Crea variable para configuracion de lectura del archivo 
   val CurrentSetting = new huemul_DataLakeSetting(huemulBigDataGov) 
 
   //setea la fecha de vigencia de esta configuracion 
   CurrentSetting.StartDate = huemulBigDataGov.setDateTime(2019,1,1,0,0,0) 
   CurrentSetting.EndDate = huemulBigDataGov.setDateTime(2021,2,1,0,0,0) 
    
   //Configuracion de rutas globales 
   CurrentSetting.GlobalPath = huemulBigDataGov.GlobalSettings.RAW_SmallFiles_Path 
 
   //Configura ruta local, se pueden usar comodines 
   CurrentSetting.LocalPath = "sgt/" 
 
   //configura el nombre del archivo (se pueden usar comodines) 
   CurrentSetting.FileName = "GDD_M_SGT_Cuentas_Contables_{{YYYY}}{{MM}}.dat" 
 
   //especifica el tipo de archivo a leer 
   CurrentSetting.FileType = huemulType_FileType.TEXT_FILE 
 
   //expecifica el nombre del contacto del archivo en TI 
   CurrentSetting.ContactName = "Herman Sotomayor" 
 
   //Indica como se lee el archivo 
   CurrentSetting.DataSchemaConf.ColSeparatorType = huemulType_Separator.CHARACTER  //POSITION;CHARACTER 
    
   //separador de columnas 
   CurrentSetting.DataSchemaConf.ColSeparator = "\\|\\|"    //SET FOR CARACTER 
   
    
   //========================================================= 
   // Auto-Gen columns information 
   //=========================================================  
  
CurrentSetting.DataSchemaConf.AddColumns("id_Cuenta_Contable","id_Cuenta_Contable",StringType,"id_Cuenta_Contable")

CurrentSetting.DataSchemaConf.AddColumns("producto_nom","producto_nom",StringType,"producto_nom")

CurrentSetting.DataSchemaConf.AddColumns("Parent_Id","Parent_Id",StringType,"Parent_Id")

CurrentSetting.DataSchemaConf.AddColumns("Tipo","Tipo",StringType,"Tipo")
   
 
  //========================================================== 
  // Auto-Gen Log schema if applies 
  // ========================================================= 
 
   CurrentSetting.LogSchemaConf.ColSeparatorType = huemulType_Separator.CHARACTER 
   CurrentSetting.LogNumRows_FieldName = null 
   CurrentSetting.LogSchemaConf.ColSeparator = "\\|\\|"  
   CurrentSetting.LogSchemaConf.setHeaderColumnsString( "VACIO" ) 
    
 
  //--Cabecera 
   
    CurrentSetting.LogSchemaConf.AddColumns("Cabecera negocio","Cabecera logico",StringType,"Es una cabecera",0,0)  
	 
   //apply configuration 
   this.SettingByDate.append(CurrentSetting) 
    
      
    /*** 
   * open(ano: Int, mes: Int) <br> 
   * metodo que retorna una estructura con un DF de detalle, y registros de control <br> 
   * ano: anio de los archivos recibidos <br> 
   * mes: mes de los archivos recibidos <br> 
   * dia: dia de los archivos recibidos <br> 
   * Retorna: true si todo esta OK, false si tuvo algun problema <br> 
  */ 
  def open(Alias: String, ControlParent: huemul_Control, ano: Integer, mes: Integer, dia: Integer, hora: Integer, min: Integer, seg: Integer): Boolean = { 
    //Crea registro de control de procesos 
     val control = new huemul_Control(huemulBigDataGov, ControlParent, huemulType_Frequency.ANY_MOMENT) 
      
    //Guarda los parametros importantes en el control de procesos 
    control.AddParamYear("Ano", ano) 
    control.AddParamMonth("Mes", mes) 
    control.AddParamMonth("Dia", dia) 
       
    try {  
      //NewStep va registrando los pasos de este proceso, tambien sirve como documentacion del mismo. 
      control.NewStep("Abre archivo RDD y devuelve esquemas para transformar a DF") 
 
      if (!this.OpenFile(ano, mes, dia, hora, min, seg, null)){ 
        //Control tambien entrega mecanismos de envio de excepciones 
        control.RaiseError(s"Error al abrir archivo: ${this.Error.ControlError_Message}") 
      } 
    
      control.NewStep("Aplicando Filtro") 
      //Si el archivo no tiene cabecera, comentar la linea de .filter 
 
      val rowRDD = this.DataRDD 
            //filtro para considerar solo las filas que los dos primeros caracteres son numericos 
            .filter { x => x.length()>=4 && huemulBigDataGov.isAllDigits(x.substring(0, 2) )  } 
 
            //filtro para dejar fuera la primera fila 
            .filter { x => x != this.Log.DataFirstRow  } 
            .map { x => this.ConvertSchema(x) } 
             
      control.NewStep("Transformando datos a dataframe")       
 
      //Crea DataFrame en Data.DataDF 
      this.DF_from_RAW(rowRDD, Alias) 
                           
      control.FinishProcessOK                       
    } catch { 
      case e: Exception => { 
        control.Control_Error.GetError(e, this.getClass.getName, null) 
        control.FinishProcessError()    
      } 
    }          
    return control.Control_Error.IsOK() 
  } 
} 
 
 
/** 
 * Este objeto se utiliza solo para probar la lectura del archivo RAW 
 * La clase que esta definida mas abajo se utiliza para la lectura. 
 */ 
object raw_dim_cuentas_contables_test { 
/** 
   * El proceso main es invocado cuando se ejecuta este codigo 
   * Permite probar la configuracion del archivo RAW 
   */ 
   
  def main(args : Array[String]) { 
     
    //Creacion API 
    val huemulBigDataGov  = new huemul_BigDataGovernance(s"Testing DataLake - ${this.getClass.getSimpleName}", args, Global) 
    //Creacion del objeto control, por default no permite ejecuciones en paralelo del mismo objeto (corre en modo SINGLETON) 
    val Control = new huemul_Control(huemulBigDataGov, null, huemulType_Frequency.MONTHLY) 
     
    /*************** PARAMETROS **********************/ 
    var param_ano = huemulBigDataGov.arguments.GetValue("year", null, "Debe especificar el parametro anio, ej: ano=2017").toInt 
    var param_mes = huemulBigDataGov.arguments.GetValue("month", null, "Debe especificar el parametro mes, ej: mes=12").toInt 
     
    //Inicializa clase RAW   
    val DF_RAW =  new raw_dim_cuentas_contables(huemulBigDataGov, Control) 
    if (!DF_RAW.open("DF_RAW", null, param_ano, param_mes, 0, 0, 0, 0)) { 
      println("************************************************************") 
      println("**********  E  R R O R   E N   P R O C E S O   *************") 
      println("************************************************************") 
    } else 
      DF_RAW.DataFramehuemul.DataFrame.show() 
       
    Control.FinishProcessOK 
    
  }   
} 

