package cl.bancochile.fdd.tables.master 
 
import com.huemulsolutions.bigdata.common._ 
import com.huemulsolutions.bigdata.control._ 
import com.huemulsolutions.bigdata.tables._ 
import com.huemulsolutions.bigdata.dataquality._ 
import org.apache.spark.sql.types._ 
 
 
class tbl_activos1_messys (huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_Table(huemulBigDataGov, Control) with Serializable { 
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
//  this.WhoCanRun_executeFull_addAccess("process_activos1_mes", 
//                                       "cl.bancochile.fdd.master.cmf.process") 
  //Solo estos package y clases pueden ejecutar en modo solo Insert, si no se especifica todos pueden invocar 
//  this.WhoCanRun_executeOnlyInsert_addAccess("process_activos1_mes", 
//                                        "cl.bancochile.fdd.master.cmf.process") 
  //Solo estos package y clases pueden ejecutar en modo solo Update, si no se especifica todos pueden invocar 
//  this.WhoCanRun_executeOnlyUpdate_addAccess("process_activos1_mes", 
//                                        "cl.bancochile.fdd.master.cmf.process") 
   
/**********   Columns Information   ****************************************/ 
    
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

    val Coloc_total = new huemul_Columns (DoubleType,true,"") 
    Coloc_total.setNullable(true) 
    Coloc_total.setIsPK(false) 
   
    val Efec_depos_bancos = new huemul_Columns (DoubleType,true,"") 
    Efec_depos_bancos.setNullable(true) 
    Efec_depos_bancos.setIsPK(false) 
   
    val Instr_fin_no_deriv = new huemul_Columns (DoubleType,true,"") 
    Instr_fin_no_deriv.setNullable(true) 
    Instr_fin_no_deriv.setIsPK(false) 
   
    val Instr_fin_deriv = new huemul_Columns (DoubleType,true,"") 
    Instr_fin_deriv.setNullable(true) 
    Instr_fin_deriv.setIsPK(false) 
   
    val Contr_retrocompra_prest = new huemul_Columns (DoubleType,true,"") 
    Contr_retrocompra_prest.setNullable(true) 
    Contr_retrocompra_prest.setIsPK(false) 
   
    val Inv_soc_sucur = new huemul_Columns (DoubleType,true,"") 
    Inv_soc_sucur.setNullable(true) 
    Inv_soc_sucur.setIsPK(false) 
   
    val Act_fijo = new huemul_Columns (DoubleType,true,"") 
    Act_fijo.setNullable(true) 
    Act_fijo.setIsPK(false) 
   
    val Act_der_bien_arren = new huemul_Columns (DoubleType,true,"") 
    Act_der_bien_arren.setNullable(true) 
    Act_der_bien_arren.setIsPK(false) 
   
    val Act_tot = new huemul_Columns (DoubleType,true,"") 
    Act_tot.setNullable(true) 
    Act_tot.setIsPK(false) 
    Act_tot.setDQ_MinDecimalValue(Decimal.apply(-10),"COD_ERROR") // para Activos1 en 202006 encontramos valor -10 para Banco Nacional de Pueblo 
    Act_tot.setDQ_MaxDecimalValue(Decimal.apply(100000000),"COD_ERROR")

    val Cred_cont = new huemul_Columns (DoubleType,true,"") 
    Cred_cont.setNullable(true) 
    Cred_cont.setIsPK(false) 
   
    val Coloc_com_ext_tot = new huemul_Columns (DoubleType,true,"") 
    Coloc_com_ext_tot.setNullable(true) 
    Coloc_com_ext_tot.setIsPK(false) 
   
    val Oper_leas_tot = new huemul_Columns (DoubleType,true,"") 
    Oper_leas_tot.setNullable(true) 
    Oper_leas_tot.setIsPK(false) 
   
    val Oper_factoraje = new huemul_Columns (DoubleType,true,"") 
    Oper_factoraje.setNullable(true) 
    Oper_factoraje.setIsPK(false) 
   
    val Car_mor_90_dia = new huemul_Columns (DoubleType,true,"") 
    Car_mor_90_dia.setNullable(true) 
    Car_mor_90_dia.setIsPK(false) 
   
    val Car_deterio = new huemul_Columns (DoubleType,true,"") 
    Car_deterio.setNullable(true) 
    Car_deterio.setIsPK(false) 
 
     
  this.ApplyTableDefinition() 
} 
