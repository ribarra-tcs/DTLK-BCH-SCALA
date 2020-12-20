package cl.bancochile.fdd.master.cmf.raw 
         
//Project's global setting 
import cl.bancochile.fdd.todos.globalSettings._ 
 
import com.huemulsolutions.bigdata.common._ 
import com.huemulsolutions.bigdata.control._ 
import com.huemulsolutions.bigdata.datalake._ 
import com.huemulsolutions.bigdata.tables._ 
import org.apache.spark.sql.types._ 
import com.huemulsolutions.bigdata.control.huemulType_Frequency._ 
import com.huemulsolutions.bigdata.dataquality._
import com.huemulsolutions.bigdata.dataquality.huemul_DQRecord
 
/** 
 * Clase que permite abrir un archivo de texto, devuelve un objeto huemul_dataLake con un DataFrame de los datos 
 * ejemplo de nombre: raw_institucion_mes 
 */ 
class raw_activos1_mes(huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_DataLake(huemulBigDataGov, Control) with Serializable  { 
   this.Description = "Activos 1" 
   this.GroupName = ("cmf").toUpperCase() 
    
   //Crea variable para configuracion de lectura del archivo 
   val CurrentSetting = new huemul_DataLakeSetting(huemulBigDataGov) 
 
   //setea la fecha de vigencia de esta configuracion 
   CurrentSetting.StartDate = huemulBigDataGov.setDateTime(2019,1,1,0,0,0) 
   CurrentSetting.EndDate = huemulBigDataGov.setDateTime(2021,2,1,0,0,0) 
    
   //Configuracion de rutas globales 
   CurrentSetting.GlobalPath = huemulBigDataGov.GlobalSettings.RAW_SmallFiles_Path 
 
   //Configura ruta local, se pueden usar comodines 
   CurrentSetting.LocalPath = "cmf/" 
 
   //configura el nombre del archivo (se pueden usar comodines) 
   //CurrentSetting.FileName = "GDD_M_CMF_Activos1_{{YYYY}}{{MM}}{{DD}}.dat" 
     CurrentSetting.FileName = "GDD_M_CMF_Activos1_{{YYYY}}{{MM}}.dat"  
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
 
 
     
 
  //--demo 
 
  
CurrentSetting.DataSchemaConf.AddColumns("Institucion","Institucion",StringType,"Institucion")

CurrentSetting.DataSchemaConf.AddColumns("Coloc_total","Coloc_total",DecimalType(15,4),"Coloc_total")

CurrentSetting.DataSchemaConf.AddColumns("Efec_depos_bancos","Efec_depos_bancos",DecimalType(15,4),"Efec_depos_bancos")

CurrentSetting.DataSchemaConf.AddColumns("Instr_fin_no_deriv","Instr_fin_no_deriv",DecimalType(15,4),"Instr_fin_no_deriv")

CurrentSetting.DataSchemaConf.AddColumns("Instr_fin_deriv","Instr_fin_deriv",DecimalType(15,4),"Instr_fin_deriv")

CurrentSetting.DataSchemaConf.AddColumns("Contr_retrocompra_prest","Contr_retrocompra_prest",DecimalType(15,4),"Contr_retrocompra_prest")

CurrentSetting.DataSchemaConf.AddColumns("Inv_soc_sucur","Inv_soc_sucur",DecimalType(15,4),"Inv_soc_sucur")

CurrentSetting.DataSchemaConf.AddColumns("Act_fijo","Act_fijo",DecimalType(15,4),"Act_fijo")

CurrentSetting.DataSchemaConf.AddColumns("Act_der_bien_arren","Act_der_bien_arren",DecimalType(15,4),"Act_der_bien_arren")

CurrentSetting.DataSchemaConf.AddColumns("Act_tot","Act_tot",DecimalType(15,4),"Act_tot")

CurrentSetting.DataSchemaConf.AddColumns("Cred_cont","Cred_cont",DecimalType(15,4),"Cred_cont")

CurrentSetting.DataSchemaConf.AddColumns("Coloc_com_ext_tot","Coloc_com_ext_tot",DecimalType(15,4),"Coloc_com_ext_tot")

CurrentSetting.DataSchemaConf.AddColumns("Oper_leas_tot","Oper_leas_tot",DecimalType(15,4),"Oper_leas_tot")

CurrentSetting.DataSchemaConf.AddColumns("Oper_factoraje","Oper_factoraje",DecimalType(15,4),"Oper_factoraje")

CurrentSetting.DataSchemaConf.AddColumns("Car_mor_90_dia","Car_mor_90_dia",DecimalType(15,4),"Car_mor_90_dia")

CurrentSetting.DataSchemaConf.AddColumns("Car_deterio","Car_deterio",DecimalType(15,4),"Car_deterio")
   
 
  //========================================================== 
  // Auto-Gen Log schema if applies 
  // ========================================================= 
 
   CurrentSetting.LogSchemaConf.ColSeparatorType = huemulType_Separator.CHARACTER 
   CurrentSetting.LogNumRows_FieldName = null 
   CurrentSetting.LogSchemaConf.ColSeparator = "||"  
   CurrentSetting.LogSchemaConf.setHeaderColumnsString( "VACIO" ) 
    
 
  //--Cabecera 
   
    CurrentSetting.LogSchemaConf.AddColumns("Cabecera negocio","Cabecera logico",StringType,"Es una cabecera",0,0) 
   
   
  //--Pie 
   
 
	 
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

      val cntRDD = this.DataRDD
                   .filter{x=> x == this.Log.DataFirstRow}.map(row => row.split("\\|\\|")(6))
      
        val hdrCnt:Int = cntRDD.first().toInt

      val rowRDD = this.DataRDD 
            //filtro para considerar solo las filas que los dos primeros caracteres son numericos 
//            .filter { x => x.length()>=4 && huemulBigDataGov.isAllDigits(x.substring(0, 2) )  } 
 
            //filtro para dejar fuera la primera fila 
            .filter { x => x != this.Log.DataFirstRow  } 
            .map { x => this.ConvertSchema(x) } 
      

       //****VALIDACION DQ*****
      //**********************
      control.NewStep("FILE RECON PROCESS: Valida que cantidad de registros estÃ© entre Header rec count y Header Rec Count")    
      //validacion cantidad de filas
     // val validanumfilas = this.DataFramehuemul.DQ_NumRowsInterval(this, hdrCnt, hdrCnt)      

 def recon_DQ(ObjectData: Object, NumRowsExpected: Long, DQ_ExternalCode:String = null): huemul_DataQualityResult = {
    val dt_start = huemulBigDataGov.getCurrentDateTimeJava()
    val DQResult = new huemul_DataQualityResult()
    var fileRows = rowRDD.count()
    if (fileRows == NumRowsExpected ) {
      DQResult.isError = false
    } else if (fileRows < NumRowsExpected) {
      DQResult.isError = true
      DQResult.Description = s"huemul_DataFrame Error: Rows(${fileRows}) < NumExpected($NumRowsExpected)"
      DQResult.Error_Code = 2003
    } else if (fileRows > NumRowsExpected) {
      DQResult.isError = true
      DQResult.Description = s"huemul_DataFrame Error: Rows(${fileRows}) > NumExpected($NumRowsExpected)"
      DQResult.Error_Code = 2004
    }
   DQResult
 }
     val validanumfilas = recon_DQ(rowRDD,hdrCnt,null)

//     val validanumfilas = this.DataFramehuemul.DQ_NumRows(rowRDD,hdrCnt,null)
      if (validanumfilas.isError)
      {
       control.RaiseError(s"user: Numero de Filas fuera del rango. ${validanumfilas.Description}")
      }




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
object raw_activos1_mes_test { 
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
    val DF_RAW =  new raw_activos1_mes(huemulBigDataGov, Control) 
    if (!DF_RAW.open("DF_RAW", null, param_ano, param_mes, 0, 0, 0, 0)) { 
      println("************************************************************") 
      println("**********  E  R R O R   E N   P R O C E S O   *************") 
      println("************************************************************") 
    } else 
      DF_RAW.DataFramehuemul.DataFrame.show() 
       
    Control.FinishProcessOK 
    
  }   
} 

