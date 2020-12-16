package cl.bancochile.fdd.dim.sgt.process 

 
//Project's global setting  
import cl.bancochile.fdd.todos.globalSettings._ 
 
import cl.bancochile.fdd.tables.dim._

import com.huemulsolutions.bigdata.common._ 
import com.huemulsolutions.bigdata.control._ 
import java.util.Calendar; 
import org.apache.spark.sql.types._ 
 
 
object process_fac_operacion { 
   
  /** 
   * Este codigo se ejecuta cuando se llama el JAR desde spark2-submit. el codigo esta preparado para hacer reprocesamiento masivo. 
  */ 
  def main(args : Array[String]) { 
    //Creacion API 
    val huemulBigDataGov  = new huemul_BigDataGovernance(s"fact operations Dimension tabla load - ${this.getClass.getSimpleName}", args, Global) 
     
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
       
      /*************** Loading of Dimension Layer from Master for fac_deudor table **********************/ 
      Control.NewStep("Sourcing data from Activos2 Master table for loading fac_operacion") 
       
	  
       val proc_date_new : String = huemulBigDataGov.arguments.GetValue("year", null, "Debe especificar el parametro anio,ej:year=2017").toString.concat(huemulBigDataGov.arguments.GetValue("month", null, "Debe especificar el parametro month, ej: month=12")).toString 
       
      /*********************************************************/ 
      /*************** Accessing Master Tables **********************/ 
      /*********************************************************/ 
      Control.NewStep("Accessing Master Layer to retrive Data for Fac Operacion table of CMF-Dimension Layer ") 
      
     // val Df1 = huemulBigDataGov.spark.sql(s"""select x.institucion, x.periodo_mes, x.id_interfaz, expl.id_cc, expl.cc_mon from (select institucion, periodo_mes, id_interfaz, map("14310 01 02", Act_adeu_bancos_pais_prest_interb, "14310 01 04", Act_adeu_bancos_pais_cred_com_ext,"14315 01 00", Act_adeu_bancos_pais_prov,"14320 01 02", Act_adeu_bancos_ext_prest_interb,"14320 01 04", Act_adeu_bancos_ext_cred_com_ext,"14325 01 00", Act_adeu_bancos_ext_prov,"14330 01 00", Act_adeu_bancos_bcentral,     "14500 00 10", Act_cred_cpcac_coloccom_coloc,"14950 01 00", Act_cred_cpcac_coloccom_prov,"14800 01 00", Act_cred_cpcac_pers_cons_cuotas,"14800 03 00", Act_cred_cpcac_pers_cons_tarcred,"14800 09 00", Act_cred_cpcac_pers_cons_otros,"14600 01 00", Act_cred_cpcac_pers_viv_coloc,"14100 00 00",Coloc_total) as cc_num from production_master.tbl_activos2_messys ) x lateral view explode(cc_num) expl as id_cc, cc_mon""")

           val Df1 = huemulBigDataGov.spark.sql(s"""select x.institucion, x.periodo_mes, x.id_interfaz, expl.id_cc, expl.cc_mon from (select institucion, periodo_mes, id_interfaz, map("14310 01 02", Act_adeu_bancos_pais_prest_interb, "14310 01 04", Act_adeu_bancos_pais_cred_com_ext,"14315 01 00", Act_adeu_bancos_pais_prov,"14320 01 02", Act_adeu_bancos_ext_prest_interb,"14320 01 04", Act_adeu_bancos_ext_cred_com_ext,"14325 01 00", Act_adeu_bancos_ext_prov,"14330 01 00", Act_adeu_bancos_bcentral,     "14500 00 10", Act_cred_cpcac_coloccom_coloc,"14950 01 00", Act_cred_cpcac_coloccom_prov,"14800 01 00", Act_cred_cpcac_pers_cons_cuotas,"14800 03 00", Act_cred_cpcac_pers_cons_tarcred,"14800 09 00", Act_cred_cpcac_pers_cons_otros,"14600 01 00", Act_cred_cpcac_pers_viv_coloc,"14100 00 00",Coloc_total) as cc_num from production_master.tbl_activos2_messys where periodo_mes="$proc_date_new") x lateral view explode(cc_num) expl as id_cc, cc_mon""")

     Df1.createOrReplaceTempView("df_final") 

     Df1.show()

       //-Creation output tables 
       
      /*********************************************************/ 
      /*************** DATOS DE fac_operacion ************/ 
      /*********************************************************/ 
       
	   Control.NewStep("Loading Dimension Layer - fac_operacion") 
     
        val huemulTable_tbl_fac_operacion = new tbl_fac_operacion(huemulBigDataGov,Control) 
        huemulTable_tbl_fac_operacion.DF_from_SQL("tbl_fac_operacion_cmf","""SELECT * FROM df_final""") 


       
        huemulTable_tbl_fac_operacion.id_interfaz.SetMapping("id_interfaz")
	huemulTable_tbl_fac_operacion.periodo_mes.SetMapping("periodo_mes")
	huemulTable_tbl_fac_operacion.id_institucion.SetMapping("institucion")
	huemulTable_tbl_fac_operacion.id_cc.SetMapping("id_cc")
	huemulTable_tbl_fac_operacion.cc_mon.SetMapping("cc_mon")
		
		                  
	if (!huemulTable_tbl_fac_operacion.executeFull("DF_tbl_fac_operacion_Final")){ 
	     Control.RaiseError(s"User: Error trying in loading dimensional table huemulTable_tbl_fac_operacion (${huemulTable_tbl_fac_operacion.Error_Code}): ${huemulTable_tbl_fac_operacion.Error_Text}") 
        } 
       
             
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
 

