package cl.bancochile.fdd.tables.master 
 
import com.huemulsolutions.bigdata.common._ 
import com.huemulsolutions.bigdata.control._ 
import com.huemulsolutions.bigdata.tables._ 
import com.huemulsolutions.bigdata.dataquality._ 
import org.apache.spark.sql.types._ 
 
 
class tbl_activos2_messys (huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_Table(huemulBigDataGov, Control) with Serializable { 
  /**********   C O N F I G U R A C I O N   D E   L A   T A B L A   ****************************************/ 
  //Tipo de tabla, Master y Reference son catalogos sin particiones de periodo 
  this.setTableType(huemulType_Tables.Transaction) 
 
  //Base de Datos en HIVE donde sera creada la tabla 
  this.setDataBase(huemulBigDataGov.GlobalSettings.MASTER_DataBase) 
 
  //Tipo de archivo que sera almacenado en HDFS 
  this.setStorageType(huemulType_StorageType.PARQUET) 
 
  //Ruta en HDFS donde se guardara el archivo PARQUET 
  this.setGlobalPaths(huemulBigDataGov.GlobalSettings.MASTER_BigFiles_Path) 
 
  //Ruta en HDFS especifica para esta tabla (Globalpaths / localPath) 
  this.setLocalPath("cmf/") 
 
  //Frecuencia de actualizacion 
  this.setFrequency(huemulType_Frequency.MONTHLY) 
   
  /**********   S E T E O   I N F O R M A T I V O   ****************************************/ 
  //Descripcion de la fuente 
  this.setDescription("") 
 
  //Nombre del contacto de negocio  
  this.setBusiness_ResponsibleName("") 
 
  //Nombre del contacto de TI 
  this.setIT_ResponsibleName("") 
    
  
  /**********   S E G U R I D A D   ****************************************/ 
  //Solo estos package y clases pueden ejecutar en modo full, si no se especifica todos pueden invocar 
//  this.WhoCanRun_executeFull_addAccess("process_activos2_mes", 
//                                       "cl.bancochile.fdd.master.cmf.process") 
  //Solo estos package y clases pueden ejecutar en modo solo Insert, si no se especifica todos pueden invocar 
//  this.WhoCanRun_executeOnlyInsert_addAccess("process_activos2_mes", 
//                                        "cl.bancochile.fdd.master.cmf.process") 
  //Solo estos package y clases pueden ejecutar en modo solo Update, si no se especifica todos pueden invocar 
//  this.WhoCanRun_executeOnlyUpdate_addAccess("process_activos2_mes", 
//                                        "cl.bancochile.fdd.master.cmf.process") 
   
/**********   Columns Information   ****************************************/ 
  //If table is Transaction. period must be create 
    

    val id_interfaz = new huemul_Columns (IntegerType,true,"")
    id_interfaz.setNullable(false)
    id_interfaz.setIsPK(true)
   
    val Institucion = new huemul_Columns (StringType,true,"") 
    Institucion.setNullable(false) 
    Institucion.setIsPK(true) 
  
    val periodo_mes = new huemul_Columns (StringType,true,"")
    periodo_mes.setNullable(false)
    periodo_mes.setIsPK(true)
    periodo_mes.setPartitionColumn(1, dropBeforeInsert = true, oneValuePerProcess = true)

    val Act_adeu_bancos_totaln = new huemul_Columns (DoubleType,true,"") 
    Act_adeu_bancos_totaln.setNullable(true) 
    Act_adeu_bancos_totaln.setIsPK(false) 
   
    val Act_adeu_bancos_pais_total = new huemul_Columns (DoubleType,true,"") 
    Act_adeu_bancos_pais_total.setNullable(true) 
    Act_adeu_bancos_pais_total.setIsPK(false) 
   
    val Act_adeu_bancos_pais_prest_interb = new huemul_Columns (DoubleType,true,"") 
    Act_adeu_bancos_pais_prest_interb.setNullable(true) 
    Act_adeu_bancos_pais_prest_interb.setIsPK(false) 
   
    val Act_adeu_bancos_pais_cred_com_ext = new huemul_Columns (DoubleType,true,"") 
    Act_adeu_bancos_pais_cred_com_ext.setNullable(true) 
    Act_adeu_bancos_pais_cred_com_ext.setIsPK(false) 
   
    val Act_adeu_bancos_pais_prov = new huemul_Columns (DoubleType,true,"") 
    Act_adeu_bancos_pais_prov.setNullable(true) 
    Act_adeu_bancos_pais_prov.setIsPK(false) 
   
    val Act_adeu_bancos_ext_total = new huemul_Columns (DoubleType,true,"") 
    Act_adeu_bancos_ext_total.setNullable(true) 
    Act_adeu_bancos_ext_total.setIsPK(false) 
   
    val Act_adeu_bancos_ext_prest_interb = new huemul_Columns (DoubleType,true,"") 
    Act_adeu_bancos_ext_prest_interb.setNullable(true) 
    Act_adeu_bancos_ext_prest_interb.setIsPK(false) 
   
    val Act_adeu_bancos_ext_cred_com_ext = new huemul_Columns (DoubleType,true,"") 
    Act_adeu_bancos_ext_cred_com_ext.setNullable(true) 
    Act_adeu_bancos_ext_cred_com_ext.setIsPK(false) 
   
    val Act_adeu_bancos_ext_prov = new huemul_Columns (DoubleType,true,"") 
    Act_adeu_bancos_ext_prov.setNullable(true) 
    Act_adeu_bancos_ext_prov.setIsPK(false) 
   
    val Act_adeu_bancos_bcentral = new huemul_Columns (DoubleType,true,"") 
    Act_adeu_bancos_bcentral.setNullable(true) 
    Act_adeu_bancos_bcentral.setIsPK(false) 
   
    val Act_cred_cpcac_total = new huemul_Columns (DoubleType,true,"") 
    Act_cred_cpcac_total.setNullable(true) 
    Act_cred_cpcac_total.setIsPK(false) 
   
    val Act_cred_cpcac_prov = new huemul_Columns (DoubleType,true,"") 
    Act_cred_cpcac_prov.setNullable(true) 
    Act_cred_cpcac_prov.setIsPK(false) 
   
    val Act_cred_cpcac_coloccom_coloc = new huemul_Columns (DoubleType,true,"") 
    Act_cred_cpcac_coloccom_coloc.setNullable(true) 
    Act_cred_cpcac_coloccom_coloc.setIsPK(false) 
   
    val Act_cred_cpcac_coloccom_prov = new huemul_Columns (DoubleType,true,"") 
    Act_cred_cpcac_coloccom_prov.setNullable(true) 
    Act_cred_cpcac_coloccom_prov.setIsPK(false) 
   
    val Act_cred_cpcac_pers_total = new huemul_Columns (DoubleType,true,"") 
    Act_cred_cpcac_pers_total.setNullable(true) 
    Act_cred_cpcac_pers_total.setIsPK(false) 
	
	val Act_cred_cpcac_pers_prov = new huemul_Columns (DoubleType,true,"") 
    Act_cred_cpcac_pers_prov.setNullable(true) 
    Act_cred_cpcac_pers_prov.setIsPK(false) 
	
	val Act_cred_cpcac_pers_cons_total = new huemul_Columns (DoubleType,true,"") 
    Act_cred_cpcac_pers_cons_total.setNullable(true) 
    Act_cred_cpcac_pers_cons_total.setIsPK(false) 
	
	val Act_cred_cpcac_pers_cons_cuotas = new huemul_Columns (DoubleType,true,"") 
    Act_cred_cpcac_pers_cons_cuotas.setNullable(true) 
    Act_cred_cpcac_pers_cons_cuotas.setIsPK(false) 
	
	val Act_cred_cpcac_pers_cons_tarcred = new huemul_Columns (DoubleType,true,"") 
    Act_cred_cpcac_pers_cons_tarcred.setNullable(true) 
    Act_cred_cpcac_pers_cons_tarcred.setIsPK(false) 
	
	val Act_cred_cpcac_pers_cons_otros = new huemul_Columns (DoubleType,true,"") 
    Act_cred_cpcac_pers_cons_otros.setNullable(true) 
    Act_cred_cpcac_pers_cons_otros.setIsPK(false) 
	
	val Act_cred_cpcac_pers_cons_prov = new huemul_Columns (DoubleType,true,"") 
    Act_cred_cpcac_pers_cons_prov.setNullable(true) 
    Act_cred_cpcac_pers_cons_prov.setIsPK(false) 
	
	val Act_cred_cpcac_pers_viv_coloc = new huemul_Columns (DoubleType,true,"") 
    Act_cred_cpcac_pers_viv_coloc.setNullable(true) 
    Act_cred_cpcac_pers_viv_coloc.setIsPK(false) 
	
	val Act_cred_cpcac_pers_viv_prov = new huemul_Columns (DoubleType,true,"") 
    Act_cred_cpcac_pers_viv_prov.setNullable(true) 
    Act_cred_cpcac_pers_viv_prov.setIsPK(false) 
	
	val Coloc_total = new huemul_Columns (DoubleType,true,"") 
    Coloc_total.setNullable(true) 
    Coloc_total.setIsPK(false) 
   
  
    
  //-**********Ejemplo para aplicar DataQuality de Integridad Referencial 
 
     
  this.ApplyTableDefinition() 
} 
