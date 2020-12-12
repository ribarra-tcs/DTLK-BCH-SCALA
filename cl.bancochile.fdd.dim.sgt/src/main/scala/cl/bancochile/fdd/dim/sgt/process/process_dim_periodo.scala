package cl.bancochile.fdd.dim.sgt.process 
 
//Project's global setting  
import cl.bancochile.fdd.todos.globalSettings._ 
 
import cl.bancochile.fdd.tables.dim._ 
//import cl.bancochile.fdd.dim.sgt.raw._ 
import com.huemulsolutions.bigdata.common._ 
import com.huemulsolutions.bigdata.control._ 
import java.util.Calendar; 
import org.apache.spark.sql.types._ 
 
 
object process_dim_periodo { 
   
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
       
     
       
       
      /*********************************************************/ 
      /*************** LOGICAS DE NEGOCIO **********************/ 
      /*********************************************************/ 
      Control.NewStep("Generar Lógica de negocio") 
	  
	  
        val Df1 = new huemul_DataFrame(huemulBigDataGov, Control) 
        
		//Df1.DF_from_SQL("DF_TEMPORAL","""SELECT * FROM raw_dim_cuentan_contables""") 
		
		
		 var Df1_Final = new huemul_DataFrame(huemulBigDataGov, Control) 
		 
		
		// Df1_Final.DF_from_SQL("DF_TEMPORAL1",s"""SELECT CAST("$proc_date_new" as String) as proc_date,* FROM DF_TEMPORAL""") 
       
        
      //-Unpersist unnecesary data 
       
     //   raw_dim_cuentan_contables.DataFramehuemul.DataFrame.unpersist() 
       
       
       //-Creation output tables 
	   
	   
	   
       
      /*********************************************************/ 
      /*************** DATOS DE tbl_dim_periodo ************/ 
      /*********************************************************/ 
        Control.NewStep("Masterizacion de tbl_dim_periodo") 
        val huemulTable_tbl_dim_periodo = new tbl_dim_periodo(huemulBigDataGov,Control) 
        
		  huemulTable_tbl_dim_periodo.DF_from_SQL("TBL_Dim_periodo","""SELECT * FROM production_dim.tbl_temp_dim_periodo""") 
 		 
          huemulTable_tbl_dim_periodo.period_dia.SetMapping("period_dia") 
         
          huemulTable_tbl_dim_periodo.period_fec_periodo.SetMapping("period_fec_periodo") 
		  
		  huemulTable_tbl_dim_periodo.period_num_periodo.SetMapping("period_num_periodo") 
		  
		  huemulTable_tbl_dim_periodo.period_nom_dia.SetMapping("period_nom_dia") 
		  
		  huemulTable_tbl_dim_periodo.period_nom_dia_abr.SetMapping("period_nom_dia_abr")
		  
		  huemulTable_tbl_dim_periodo.period_num_dia_sem.SetMapping("period_num_dia_sem")
		  
		  huemulTable_tbl_dim_periodo.period_num_dia_mes.SetMapping("period_num_dia_mes")
		  
		  huemulTable_tbl_dim_periodo.period_num_dia_anio.SetMapping("period_num_dia_anio")
		  
		  huemulTable_tbl_dim_periodo.period_num_sem_mes.SetMapping("period_num_sem_mes")
		  
		   huemulTable_tbl_dim_periodo.period_num_sem_anio.SetMapping("period_num_sem_anio")
		   
		   huemulTable_tbl_dim_periodo.period_nom_mes.SetMapping("period_nom_mes")
		   
		   huemulTable_tbl_dim_periodo.period_nom_mes_abr.SetMapping("period_nom_mes_abr")
		   
		   huemulTable_tbl_dim_periodo.period_num_mes.SetMapping("period_num_mes")
		   
		   huemulTable_tbl_dim_periodo.period_num_anio.SetMapping("period_num_anio")
		   
		   huemulTable_tbl_dim_periodo.period_ind_habil.SetMapping("period_ind_habil")
         
        
         
          
         
       
         
         
        if (!huemulTable_tbl_dim_periodo.executeFull("DF_tbl_dim_periodo_Final")){ 
          Control.RaiseError(s"User: Error al intentar masterizar tbl_dim_periodo (${huemulTable_tbl_dim_periodo.Error_Code}): ${huemulTable_tbl_dim_periodo.Error_Text}") 
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
 

