package cl.bancochile.fdd.tables.dim 
 
import com.huemulsolutions.bigdata.common._ 
import com.huemulsolutions.bigdata.control._ 
import com.huemulsolutions.bigdata.tables._ 
import com.huemulsolutions.bigdata.dataquality._ 
import org.apache.spark.sql.types._ 
 
 
class tbl_dim_institution (huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_Table(huemulBigDataGov, Control) with Serializable {

  /**********   C O N F I G U R A C I O N   D E   L A   T A B L A   ****************************************/ 
  //Tipo de tabla, Master y Reference son catalogos sin particiones de periodo 
  this.setTableType(huemulType_Tables.Master) 
 
  //Base de Datos en HIVE donde sera creada la tabla 
  this.setDataBase(huemulBigDataGov.GlobalSettings.DIM_DataBase) 
 
  //Tipo de archivo que sera almacenado en HDFS 
  this.setStorageType(huemulType_StorageType.PARQUET) 
 
  //Ruta en HDFS donde se guardara el archivo PARQUET 
  this.setGlobalPaths(huemulBigDataGov.GlobalSettings.DIM_BigFiles_Path)  
 
  //Frecuencia de actualizacion 
  this.setFrequency(huemulType_Frequency.NOT_SPECIFIED) 
   
  /**********   S E T E O   I N F O R M A T I V O   ****************************************/ 
  //Descripcion de la fuente 
  this.setDescription("") 
 
  //Nombre del contacto de negocio  
  this.setBusiness_ResponsibleName("") 
 
  //Nombre del contacto de TI 
  this.setIT_ResponsibleName("") 
    
  
  /**********   S E G U R I D A D   ****************************************/ 
  //Solo estos package y clases pueden ejecutar en modo full, si no se especifica todos pueden invocar 
//  this.WhoCanRun_executeFull_addAccess("process_dim_cuentan_contables", 
//                                       "cl.bancochile.fdd.dim.cmf.process") 
  //Solo estos package y clases pueden ejecutar en modo solo Insert, si no se especifica todos pueden invocar 
//  this.WhoCanRun_executeOnlyInsert_addAccess("process_dim_cuentan_contables", 
//                                        "cl.bancochile.fdd.dim.cmf.process") 
  //Solo estos package y clases pueden ejecutar en modo solo Update, si no se especifica todos pueden invocar 
//  this.WhoCanRun_executeOnlyUpdate_addAccess("process_dim_cuentan_contables", 
//                                        "cl.bancochile.fdd.dim.cmf.process") 
   
/**********   Columns Information   ****************************************/ 
   
    val Id_institucion = new huemul_Columns (IntegerType,true,"") 
    Id_institucion.setNullable(false) 
    Id_institucion.setIsPK(true) 
   
    val Nombre_institucion = new huemul_Columns (StringType,true,"") 
    Nombre_institucion.setNullable(true) 
    Nombre_institucion.setIsPK(false) 
  
     
  this.ApplyTableDefinition() 
} 
