package cl.bancochile.fdd.dim.sgt.process
 
//Project's global setting  
import cl.bancochile.fdd.todos.globalSettings._ 
import org.apache.spark.sql._ 
//import cl.bancochile.fdd.tables.master._ 
//import cl.bancochile.fdd.master.cmf.raw._ 
import com.huemulsolutions.bigdata.common._ 
import com.huemulsolutions.bigdata.control._ 
import java.util.Calendar; 
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions
 
 
object process_cmf_test { 
   
  /** 
   * Este codigo se ejecuta cuando se llama el JAR desde spark2-submit. el codigo esta preparado para hacer reprocesamiento masivo. 
  */ 
  def main(args : Array[String]) { 
    //Creacion API 
    val huemulBigDataGov  = new huemul_BigDataGovernance(s"Testing of Operational Reporting of CMF - ${this.getClass.getSimpleName}", args, Global) 
     
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
   
  
  def process_master(huemulBigDataGov: huemul_BigDataGovernance, ControlParent: huemul_Control, param_year: Integer, param_month: Integer, param_day: Integer): Boolean = { 
    val Control = new huemul_Control(huemulBigDataGov, ControlParent, huemulType_Frequency.DAILY)     
     
    try {              
      /*************** AGREGAR PARAMETROS A CONTROL **********************/ 
      Control.AddParamYear("param_year", param_year) 
      Control.AddParamMonth("param_month", param_month) 
      Control.AddParamDay("param_day",param_day) 
         
      //Control.AddParamInformation("param_oters", param_otherparams) 
       
      /*************** Operational Reporting with CMF DATALAKE **********************/ 
      Control.NewStep("TESTING: CMF Operational Reporting COMPLETENESS testing started") 
       
      val Df1 = huemulBigDataGov.spark.sql(s"""select a.periodo_mes, coloc_total, b.cc_mon, c.cc_mon as activo_nom from (select periodo_mes, sum(coloc_total) as coloc_total from production_master.tbl_activos2_messys group by periodo_mes) a left join (select periodo_mes, sum(cc_mon) cc_mon from production_dim.tbl_fac_operacion where id_cc = '14100 00 00' group by periodo_mes) b on a.periodo_mes = b.periodo_mes left join (select periodo_mes, sum(cc_mon) cc_mon from production_dim.tbl_cmf_operational_report where id_cc = '14100 00 00' group by periodo_mes) c on a.periodo_mes = c.periodo_mes""")


        Df1.write.partitionBy("periodo_mes").mode(org.apache.spark.sql.SaveMode.Overwrite).format("parquet").saveAsTable("production_dim.tbl_cmf_completeness_test")

	  
	  val Df2 = huemulBigDataGov.spark.sql(s"""select periodo_mes, sum(cc_mon) / sum(activo_nom_mes_ant) from production_dim.tbl_cmf_operational_report group by periodo_mes""")
       
      Df2.write.partitionBy("periodo_mes").mode(org.apache.spark.sql.SaveMode.Overwrite).format("parquet").saveAsTable("production_dim.tbl_cmf_consistency_test")
	  
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
 


