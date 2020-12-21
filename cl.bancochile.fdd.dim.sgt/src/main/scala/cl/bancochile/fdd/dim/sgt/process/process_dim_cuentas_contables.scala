package cl.bancochile.fdd.dim.sgt.process 
 
//Project's global setting  
import cl.bancochile.fdd.todos.globalSettings._ 
 
import cl.bancochile.fdd.tables.dim._ 
import cl.bancochile.fdd.dim.sgt.raw._ 
import com.huemulsolutions.bigdata.common._ 
import com.huemulsolutions.bigdata.control._ 
import java.util.Calendar; 
import org.apache.spark.sql.types._ 
 
 
object process_dim_cuentas_contables { 
   
  /** 
   * Este codigo se ejecuta cuando se llama el JAR desde spark2-submit. el codigo esta preparado para hacer reprocesamiento masivo. 
  */ 
  def main(args : Array[String]) { 
    //Creacion API 
    val huemulBigDataGov  = new huemul_BigDataGovernance(s"Masterizacion tabla - ${this.getClass.getSimpleName}", args, Global) 
     
    /*************** PARAMETROS **********************/ 
    var param_year = huemulBigDataGov.arguments.GetValue("year", null, "Debe especificar el parametro anio, ej: year=2017").toInt 
    var param_month = huemulBigDataGov.arguments.GetValue("month", null, "Debe especificar el parametro month, ej: month=12").toInt 
    var param_day  = huemulBigDataGov.arguments.GetValue("day", null, "Debe especificar el parametro dia, ej: day=25").toInt 
    //var param_day = 1 
    val param_numMonths = huemulBigDataGov.arguments.GetValue("num_months", "1").toInt 
 
    /*************** CICLO REPROCESO MASIVO **********************/ 
    var i: Int = 1 
    var FinOK: Boolean = true 
    var Fecha = huemulBigDataGov.setDateTime(param_year, param_month, param_day, 0, 0, 0) 
     
    while (i <= param_numMonths) { 
      param_year = huemulBigDataGov.getYear(Fecha) 
      param_month = huemulBigDataGov.getMonth(Fecha) 
      println(s"Procesando Anio $param_year, month $param_month ($i de $param_numMonths)") 
       
      //Ejecuta codigo 
      var FinOK = process_master(huemulBigDataGov, null, param_year, param_month, param_day) 
       
      if (FinOK) 
        i+=1 
      else { 
        println(s"ERROR Procesando Anio $param_year, month $param_month ($i de $param_numMonths)") 
        i = param_numMonths + 1 
      } 
         
      Fecha.add(Calendar.MONTH, 1)       
    } 
     
     
    huemulBigDataGov.close 
  } 
   
  /** 
    masterizacion de archivo [[CAMBIAR]] <br> 
    param_year: anio de los datos  <br> 
    param_month: mes de los datos  <br> 
   */ 
  def process_master(huemulBigDataGov: huemul_BigDataGovernance, ControlParent: huemul_Control, param_year: Integer, param_month: Integer, param_day: Integer): Boolean = { 
    val Control = new huemul_Control(huemulBigDataGov, ControlParent, huemulType_Frequency.DAILY)     
     
    try {              
      /*************** AGREGAR PARAMETROS A CONTROL **********************/ 
      Control.AddParamYear("param_year", param_year) 
      Control.AddParamMonth("param_month", param_month) 
      Control.AddParamDay("param_day",param_day) 
         
      //Control.AddParamInformation("param_oters", param_otherparams) 
       
      /*************** ABRE RAW DESDE DATALAKE **********************/ 
      Control.NewStep("Abre DataLake") 
       
        var raw_dim_cuentas_contables = new raw_dim_cuentas_contables(huemulBigDataGov,Control) 
        if(!raw_dim_cuentas_contables.open("raw_dim_cuentas_contables",Control,param_year,param_month,param_day,0,0,0)){ 
          Control.RaiseError(s"error encontrado al tratar de abrir raw_dim_cuentas_contables , abortar: ${raw_dim_cuentas_contables.Error.ControlError_Message}") 
        } 
       
       
      /*********************************************************/ 
      /*************** LOGICAS DE NEGOCIO **********************/ 
      /*********************************************************/ 
      Control.NewStep("Generar Lógica de negocio") 
	  
	  
        val Df1 = new huemul_DataFrame(huemulBigDataGov, Control) 
        
		Df1.DF_from_SQL("DF_TEMPORAL","""SELECT * FROM raw_dim_cuentas_contables""") 
		
		
		 var Df1_Final = new huemul_DataFrame(huemulBigDataGov, Control) 
		 
		
		// Df1_Final.DF_from_SQL("DF_TEMPORAL1",s"""SELECT CAST("$proc_date_new" as String) as proc_date,* FROM DF_TEMPORAL""") 
       
        
      //-Unpersist unnecesary data 
       
        raw_dim_cuentas_contables.DataFramehuemul.DataFrame.unpersist() 
       
       
       //-Creation output tables 
       
      /*********************************************************/ 
      /*************** DATOS DE tbl_dim_cuentas_contables ************/ 
      /*********************************************************/ 
        Control.NewStep("Masterizacion de tbl_dim_cuentas_contables") 
		
        val huemulTable_tbl_dim_cuentas_contables = new tbl_dim_cuentas_contables(huemulBigDataGov,Control) 
        
		  huemulTable_tbl_dim_cuentas_contables.DF_from_SQL("TBL_Dim_cuentas_contables","""SELECT * FROM DF_TEMPORAL""") 
 		 
          huemulTable_tbl_dim_cuentas_contables.id_Cuenta_Contable.SetMapping("id_Cuenta_Contable") 
         
          huemulTable_tbl_dim_cuentas_contables.producto_nom.SetMapping("producto_nom") 
		  
		  huemulTable_tbl_dim_cuentas_contables.Parent_Id.SetMapping("Parent_Id") 
		  
		  huemulTable_tbl_dim_cuentas_contables.Tipo.SetMapping("Tipo") 
         
        
         
          
         
       
         
         
        if (!huemulTable_tbl_dim_cuentas_contables.executeOnlyInsert("DF_tbl_dim_cuentas_contables_Final")){ 
          Control.RaiseError(s"User: Error al intentar masterizar tbl_dim_cuentas_contables (${huemulTable_tbl_dim_cuentas_contables.Error_Code}): ${huemulTable_tbl_dim_cuentas_contables.Error_Text}") 
        } 
       
//       println("******************************** our log ******************************************************");
//       println(sqlSentence);
//       println(huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HIVE.getJDBC_connection(huemulBigDataGov));
//       println("******************************** our log ******************************************************");
       
      Control.FinishProcessOK 
    } catch { 
      case e: Exception => { 
        Control.Control_Error.GetError(e, this.getClass.getName, null) 
        Control.FinishProcessError() 
        throw e 
      } 
    } 
     
    return Control.Control_Error.IsOK()    
  } 
   
} 
 

