package cl.bancochile.fdd.master.cmf.process 
 
//Project's global setting  
import cl.bancochile.fdd.todos.globalSettings._ 
import org.apache.spark.sql._ 
import cl.bancochile.fdd.tables.master._ 
import cl.bancochile.fdd.master.cmf.raw._ 
import com.huemulsolutions.bigdata.common._ 
import com.huemulsolutions.bigdata.control._ 
import com.huemulsolutions.bigdata.datalake
import java.util.Calendar; 
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions
 
 
object process_activos1_mes { 
   
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
   // var parm_month = huemulBigDataGov.arguments.GetValue("month")
    //var param_day = 0 
    val param_numMonths = huemulBigDataGov.arguments.GetValue("num_months", "1").toInt 

     /***
   * Information about interfaces
   */
 var FileName = "hdfs://cluster-b54a-m/bancochile/gdd/data/raw/cmf/GDD_M_CMF_Activos1_".concat(param_year.toString).concat(param_month.toString)
 println("Filename"+FileName)

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
    val Control = new huemul_Control(huemulBigDataGov, ControlParent, huemulType_Frequency.MONTHLY)     
     
    try {              
      /*************** AGREGAR PARAMETROS A CONTROL **********************/ 
      Control.AddParamYear("param_year", param_year) 
      Control.AddParamMonth("param_month", param_month) 
      Control.AddParamDay("param_day",param_day) 
         
      //Control.AddParamInformation("param_oters", param_otherparams) 
       
      /*************** ABRE RAW DESDE DATALAKE **********************/ 
      Control.NewStep("Abre DataLake") 
       
        var raw_activos1_mes = new raw_activos1_mes(huemulBigDataGov,Control) 
          if(!raw_activos1_mes.open("raw_activos1_mes",Control,param_year,param_month,param_day,0,0,0)){ 
   //       if(!raw_activos1_mes.open("raw_activos1_mes",Control,param_year,param_month,0,0,0,0)){  
    Control.RaiseError(s"error encontrado al tratar de abrir raw_activos1_mes , abortar: ${raw_activos1_mes.Error.ControlError_Message}") 
        } 
       
      /*********************************************************/ 
      /*************** LOGICAS DE NEGOCIO **********************/ 
      /*********************************************************/ 
      Control.NewStep("Generar Lógica de negocio") 
       
    //  val Df1 = new huemul_DataFrame(huemulBigDataGov, Control) 
    //      Df1.DF_from_SQL("DF_TEMPORAL","""SELECT * FROM raw_activos1_mes""") 

    val proc_date_new : String = huemulBigDataGov.arguments.GetValue("year", null, "Debe especificar el parametro anio,ej:year=2017").toString.concat(huemulBigDataGov.arguments.GetValue("month", null, "Debe especificar el parametro month, ej: month=12")).toString
      var Df2 = raw_activos1_mes.DataFramehuemul.DataFrame.withColumn("idx",org.apache.spark.sql.functions.monotonically_increasing_id()).withColumn("periodo_mes",org.apache.spark.sql.functions.lit(s"""$proc_date_new"""))	
//Df2.show()
          Df2.createOrReplaceTempView("df_final") 
        
      //-Unpersist unnecesary data 
       
        raw_activos1_mes.DataFramehuemul.DataFrame.unpersist() 
       
       
       //-Creation output tables 
       
      /*********************************************************/ 
      /*************** DATOS DE tbl_activos1_mes ************/ 
      /*********************************************************/ 
        Control.NewStep("Masterizacion de tbl_activos1_mes") 
        val huemulTable_tbl_activos1_mes = new tbl_activos1_messys(huemulBigDataGov,Control) 
       

	//  huemulTable_tbl_activos1_mes.DF_from_SQL("TBL_ACTIVOS1_mes","""SELECT * FROM df_final""") 
 	huemulTable_tbl_activos1_mes.DF_from_SQL("TBL_ACTIVOS1_MESSYS",s"""SELECT row_number() over (order by idx) as id_interfaz,* FROM df_final""") 
 	
        huemulTable_tbl_activos1_mes.DataFramehuemul.DataFrame.select("id_interfaz","idx","periodo_mes","Institucion").show()
	 huemulTable_tbl_activos1_mes.id_interfaz.SetMapping("id_interfaz") 	 
          
         huemulTable_tbl_activos1_mes.periodo_mes.SetMapping("periodo_mes")
         huemulTable_tbl_activos1_mes.Institucion.SetMapping("Institucion") 
         
          huemulTable_tbl_activos1_mes.Coloc_total.SetMapping("Coloc_total") 
         
          huemulTable_tbl_activos1_mes.Efec_depos_bancos.SetMapping("Efec_depos_bancos") 
         
          huemulTable_tbl_activos1_mes.Instr_fin_no_deriv.SetMapping("Instr_fin_no_deriv") 
         
          huemulTable_tbl_activos1_mes.Instr_fin_deriv.SetMapping("Instr_fin_deriv") 
         
          huemulTable_tbl_activos1_mes.Contr_retrocompra_prest.SetMapping("Contr_retrocompra_prest") 
         
          huemulTable_tbl_activos1_mes.Inv_soc_sucur.SetMapping("Inv_soc_sucur") 
         
          huemulTable_tbl_activos1_mes.Act_fijo.SetMapping("Act_fijo") 
         
          huemulTable_tbl_activos1_mes.Act_der_bien_arren.SetMapping("Act_der_bien_arren") 
         
          huemulTable_tbl_activos1_mes.Act_tot.SetMapping("Act_tot") 
         
          huemulTable_tbl_activos1_mes.Cred_cont.SetMapping("Cred_cont") 
         
          huemulTable_tbl_activos1_mes.Coloc_com_ext_tot.SetMapping("Coloc_com_ext_tot") 
         
          huemulTable_tbl_activos1_mes.Oper_leas_tot.SetMapping("Oper_leas_tot") 
         
          huemulTable_tbl_activos1_mes.Oper_factoraje.SetMapping("Oper_factoraje") 
         
          huemulTable_tbl_activos1_mes.Car_mor_90_dia.SetMapping("Car_mor_90_dia") 
         
          huemulTable_tbl_activos1_mes.Car_deterio.SetMapping("Car_deterio") 
         
         
         
        if (!huemulTable_tbl_activos1_mes.executeFull("DF_tbl_activos1_mes_Final")){ 
          Control.RaiseError(s"User: Error al intentar masterizar tbl_activos1_mes (${huemulTable_tbl_activos1_mes.Error_Code}): ${huemulTable_tbl_activos1_mes.Error_Text}") 
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
 
