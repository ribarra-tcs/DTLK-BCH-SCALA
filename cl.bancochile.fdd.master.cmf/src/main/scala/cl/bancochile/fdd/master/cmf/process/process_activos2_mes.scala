package cl.bancochile.fdd.master.cmf.process 
 
//Project's global setting  
import cl.bancochile.fdd.todos.globalSettings._ 
 
import cl.bancochile.fdd.tables.master._ 
import cl.bancochile.fdd.master.cmf.raw._ 
import com.huemulsolutions.bigdata.common._ 
import com.huemulsolutions.bigdata.control._ 
import java.util.Calendar; 
import org.apache.spark.sql.types._ 
import org.apache.spark.sql.functions 

object process_activos2_mes { 
   
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
    masterizacion de archivo tbl_Activos2_mes.scala <br> 
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
         
       
      /*************** ABRE RAW DESDE DATALAKE **********************/ 
      Control.NewStep("Abre DataLake") 
       
        var raw_activos2_mes = new raw_activos2_mes(huemulBigDataGov,Control) 
        if(!raw_activos2_mes.open("raw_activos2_mes",Control,param_year,param_month,param_day,0,0,0)){ 
          Control.RaiseError(s"error encontrado al tratar de abrir raw_activos2_mes , abortar: ${raw_activos2_mes.Error.ControlError_Message}") 
        }
          
      /*********************************************************/ 
      /*************** LOGICAS DE NEGOCIO **********************/ 
      /*********************************************************/ 
      Control.NewStep("Generar Lógica de negocio") 

      val dfcast = huemulBigDataGov.spark.sql(s"""select Institucion,cast(Act_adeu_bancos_totaln as Long),cast(Act_adeu_bancos_pais_total as Long),cast(Act_adeu_bancos_pais_prest_interb as Long),cast(Act_adeu_bancos_pais_cred_com_ext as Long),cast(Act_adeu_bancos_pais_prov as Long),cast(Act_adeu_bancos_ext_total as Long),cast(Act_adeu_bancos_ext_prest_interb as Long),cast(Act_adeu_bancos_ext_cred_com_ext as Long),cast(Act_adeu_bancos_ext_prov as Long),cast(Act_adeu_bancos_bcentral as Long),cast(Act_cred_cpcac_total as Long),cast(Act_cred_cpcac_prov as Long),cast(Act_cred_cpcac_coloccom_coloc as Long),cast(Act_cred_cpcac_coloccom_prov as Long),cast(Act_cred_cpcac_pers_total as Long),cast(Act_cred_cpcac_pers_prov as Long),cast(Act_cred_cpcac_pers_cons_total as Long),cast(Act_cred_cpcac_pers_cons_cuotas as Long),cast(Act_cred_cpcac_pers_cons_tarcred as Long),cast(Act_cred_cpcac_pers_cons_otros as Long),cast(Act_cred_cpcac_pers_cons_prov as Long),cast(Act_cred_cpcac_pers_viv_coloc as Long),cast(Act_cred_cpcac_pers_viv_prov as Long),cast(Coloc_total as Long) from raw_activos2_mes""")

      val proc_date_new : String = huemulBigDataGov.arguments.GetValue("year", null, "Debe especificar el parametro anio,ej:year=2017").toString.concat(huemulBigDataGov.arguments.GetValue("month", null, "Debe especificar el parametro month, ej: month=12")).toString

      val Df2 = dfcast.withColumn("idx",org.apache.spark.sql.functions.monotonically_increasing_id()).withColumn("periodo_mes",org.apache.spark.sql.functions.lit(s"""$proc_date_new"""))

	Df2.createOrReplaceTempView("Df2_view")
	val Df2_temp = huemulBigDataGov.spark.sql(s"""SELECT row_number() over (order by idx) as id_interfaz,* FROM Df2_view""")

	Df2_temp.createOrReplaceTempView("Df2_TEMP_VIEW")

	//******* DQ: Validaciones datos ACTIVOS2 *********//
	
	val Df_Activos_tmp = huemulBigDataGov.spark.sql(s"""select t2.*,t1.coloc_total as t1coloc_tot from Df2_TEMP_VIEW t2 left outer join production_master.tbl_activos1_messys t1 on t2.id_interfaz=t1.id_interfaz and t2.Institucion = t1.institucion and t2.periodo_mes =  t1.periodo_mes""")
	
	Df_Activos_tmp.createOrReplaceTempView("Df_Activos_view")
  
        val Df_ACTIVOS2_VALIDATION = huemulBigDataGov.spark.sql(s"""select id_interfaz,periodo_mes,(t1coloc_tot-coloc_total) as Activos_Coloc_Tot_Err,(Coloc_total-(Act_adeu_bancos_pais_total+Act_adeu_bancos_ext_total+Act_cred_cpcac_total)) as Activos2_ColZ_Err,
	(Act_cred_cpcac_total-(Act_cred_cpcac_coloccom_coloc+Act_cred_cpcac_pers_total)) as Activos2_ColL_1_Err,
	(Act_adeu_bancos_pais_total-(Act_adeu_bancos_pais_prest_interb+Act_adeu_bancos_pais_cred_com_ext)) as Activos2_ColC_1_Err,
	(Act_adeu_bancos_pais_total-(Act_adeu_bancos_pais_prest_interb+Act_cred_cpcac_total+Act_adeu_bancos_ext_prest_interb-Act_adeu_bancos_ext_total-Act_adeu_bancos_bcentral)) as Activos2_ColC_2_Err,
	(act_adeu_bancos_totaln-(Act_adeu_bancos_pais_total+Act_adeu_bancos_ext_total+Act_adeu_bancos_bcentral-act_adeu_bancos_pais_prov-act_adeu_bancos_ext_prov)) as Activos2_ColB_Err,
	(Act_cred_cpcac_total-(Act_cred_cpcac_coloccom_coloc+act_cred_cpcac_pers_cons_cuotas+act_cred_cpcac_pers_cons_tarcred+act_cred_cpcac_pers_cons_otros+act_cred_cpcac_pers_viv_coloc)) as Activos2_ColL_2_Err,(Act_adeu_bancos_ext_total-(Act_adeu_bancos_ext_prest_interb + Act_adeu_bancos_ext_cred_com_ext))as Activos2_ColG_Err from Df_Activos_view""")
	
	Df_ACTIVOS2_VALIDATION.createOrReplaceTempView("Df_ACTIVOS2_VALIDATION_view")
	Df_ACTIVOS2_VALIDATION.show()

        Df_ACTIVOS2_VALIDATION.write.partitionBy("periodo_mes").mode(org.apache.spark.sql.SaveMode.Overwrite).format("parquet").saveAsTable("production_master.tbl_Activos2_Data_validations")
	  
	 Df_ACTIVOS2_VALIDATION.repartition(1).write.partitionBy("periodo_mes").mode("overwrite").format("parquet").save(s"""hdfs://10.128.0.3/bancochile/gdd/data/master/cmf/tbl_Activos2_Data_validations""")

       //********  END: DATA VALIDATIONS para ACTIVOS2 Interface *********//
  
       //-Unpersist unnecesary data 
       raw_activos2_mes.DataFramehuemul.DataFrame.unpersist() 
       
       //-Creation output tables 
       
      /*********************************************************/ 
      /*************** DATOS DE tbl_activos2_mes ************/ 
      /*********************************************************/ 
      Control.NewStep("Masterizacion de tbl_activos2_mes") 
  
      val huemulTable_tbl_activos2_mes = new tbl_activos2_messys(huemulBigDataGov,Control) 
        
      huemulTable_tbl_activos2_mes.DF_from_SQL("TBL_ACTIVOS2_messys",s"""SELECT * FROM Df2_TEMP_VIEW""")	 
  
      huemulTable_tbl_activos2_mes.DataFramehuemul.DataFrame.select("id_interfaz","Institucion","periodo_mes").show()
      huemulTable_tbl_activos2_mes.id_interfaz.SetMapping("id_interfaz")
      huemulTable_tbl_activos2_mes.periodo_mes.SetMapping("periodo_mes")
      huemulTable_tbl_activos2_mes.Institucion.SetMapping("Institucion") 
      huemulTable_tbl_activos2_mes.Act_adeu_bancos_totaln.SetMapping("Act_adeu_bancos_totaln") 
      huemulTable_tbl_activos2_mes.Act_adeu_bancos_pais_total.SetMapping("Act_adeu_bancos_pais_total") 
      huemulTable_tbl_activos2_mes.Act_adeu_bancos_pais_prest_interb.SetMapping("Act_adeu_bancos_pais_prest_interb") 
      huemulTable_tbl_activos2_mes.Act_adeu_bancos_pais_cred_com_ext.SetMapping("Act_adeu_bancos_pais_cred_com_ext") 
      huemulTable_tbl_activos2_mes.Act_adeu_bancos_pais_prov.SetMapping("Act_adeu_bancos_pais_prov") 
      huemulTable_tbl_activos2_mes.Act_adeu_bancos_ext_total.SetMapping("Act_adeu_bancos_ext_total") 
      huemulTable_tbl_activos2_mes.Act_adeu_bancos_ext_prest_interb.SetMapping("Act_adeu_bancos_ext_prest_interb") 
      huemulTable_tbl_activos2_mes.Act_adeu_bancos_ext_cred_com_ext.SetMapping("Act_adeu_bancos_ext_cred_com_ext") 
      huemulTable_tbl_activos2_mes.Act_adeu_bancos_ext_prov.SetMapping("Act_adeu_bancos_ext_prov") 
      huemulTable_tbl_activos2_mes.Act_adeu_bancos_bcentral.SetMapping("Act_adeu_bancos_bcentral") 
      huemulTable_tbl_activos2_mes.Act_cred_cpcac_total.SetMapping("Act_cred_cpcac_total") 
      huemulTable_tbl_activos2_mes.Act_cred_cpcac_prov.SetMapping("Act_cred_cpcac_prov") 
      huemulTable_tbl_activos2_mes.Act_cred_cpcac_coloccom_coloc.SetMapping("Act_cred_cpcac_coloccom_coloc") 
      huemulTable_tbl_activos2_mes.Act_cred_cpcac_coloccom_prov.SetMapping("Act_cred_cpcac_coloccom_prov") 
      huemulTable_tbl_activos2_mes.Act_cred_cpcac_pers_total.SetMapping("Act_cred_cpcac_pers_total") 
      huemulTable_tbl_activos2_mes.Act_cred_cpcac_pers_prov.SetMapping("Act_cred_cpcac_pers_prov") 
      huemulTable_tbl_activos2_mes.Act_cred_cpcac_pers_cons_total.SetMapping("Act_cred_cpcac_pers_cons_total")        
      huemulTable_tbl_activos2_mes.Act_cred_cpcac_pers_cons_cuotas.SetMapping("Act_cred_cpcac_pers_cons_cuotas") 
      huemulTable_tbl_activos2_mes.Act_cred_cpcac_pers_cons_tarcred.SetMapping("Act_cred_cpcac_pers_cons_tarcred") 
      huemulTable_tbl_activos2_mes.Act_cred_cpcac_pers_cons_otros.SetMapping("Act_cred_cpcac_pers_cons_otros") 
      huemulTable_tbl_activos2_mes.Act_cred_cpcac_pers_cons_prov.SetMapping("Act_cred_cpcac_pers_cons_prov") 
      huemulTable_tbl_activos2_mes.Act_cred_cpcac_pers_viv_coloc.SetMapping("Act_cred_cpcac_pers_viv_coloc") 
      huemulTable_tbl_activos2_mes.Act_cred_cpcac_pers_viv_prov.SetMapping("Act_cred_cpcac_pers_viv_prov") 
      huemulTable_tbl_activos2_mes.Coloc_total.SetMapping("Coloc_total") 
   
        
      if (!huemulTable_tbl_activos2_mes.executeFull("DF_tbl_activos2_mes_Final")){ 
          Control.RaiseError(s"User: Error al intentar masterizar tbl_activos2_mes (${huemulTable_tbl_activos2_mes.Error_Code}): ${huemulTable_tbl_activos2_mes.Error_Text}") 
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
 

