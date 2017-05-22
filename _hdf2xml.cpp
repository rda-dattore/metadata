#include <iostream>
#include <sstream>
#include <memory>
#include <deque>
#include <sstream>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <hdf.hpp>
#include <netcdf.hpp>
#include <metadata.hpp>
#include <strutils.hpp>
#include <utils.hpp>

metautils::Directives directives;
metautils::Args args;
std::string user=getenv("USER");
TempFile *tfile=NULL;
TempDir *tdir=NULL;
TempFile *inv_file=nullptr;
std::ofstream inv;
struct ScanData {
  ScanData() : map_name(),varlist(),var_changes_table(),found_map(false) {}

  std::string map_name;
  std::list<std::string> varlist;
  my::map<metautils::StringEntry> var_changes_table;
  bool found_map;
};
struct ParameterData {
  ParameterData() : table(),map() {}

  my::map<metautils::StringEntry> table;
  ParameterMap map;
};
my::map<metadata::GrML::GridEntry> *grid_table=nullptr;
metadata::GrML::GridEntry *gentry;
metadata::GrML::LevelEntry *lentry;
metadata::GrML::ParameterEntry *param_entry;
my::map<metadata::ObML::IDEntry> **ID_table=nullptr;
my::map<metadata::ObML::PlatformEntry> *platform_table;
size_t num_not_missing=0;
std::string cmd_type="";
enum {GrML_type=1,ObML_type};
int write_type=-1;
metautils::NcTime::Time nctime;
metautils::NcTime::TimeBounds time_bounds;
metautils::NcTime::TimeData time_data,fcst_ref_time_data;
struct InvEntry {
  InvEntry() : key(),num(0) {}

  std::string key;
  int num;
};
my::map<InvEntry> inv_U_table,inv_G_table,inv_L_table,inv_P_table,inv_R_table;
std::list<std::string> inv_lines;
std::string myerror="";
std::stringstream wss;

extern "C" void cleanUp()
{
  if (tfile != NULL) {
    delete tfile;
  }
  if (tdir != NULL) {
    delete tdir;
  }
  if (wss.str().length() > 0) {
    metautils::logWarning(wss.str(),"hdf2xml",user,args.argsString);
  }
  if (myerror.length() > 0) {
    metautils::logError(myerror,"hdf2xml",user,args.argsString);
  }
}

void parseArgs()
{
  args.overridePrimaryCheck=false;
  args.updateDB=true;
  args.updateSummary=true;
  args.regenerate=true;
  std::deque<std::string> sp=strutils::split(args.argsString,"%");
  for (size_t n=0; n < sp.size(); ++n) {
    if (sp[n] == "-d") {
	args.dsnum=sp[++n];
	if (strutils::has_beginning(args.dsnum,"ds")) {
	  args.dsnum=args.dsnum.substr(2);
	}
    }
    else if (sp[n] == "-f") {
	args.format=sp[++n];
    }
    else if (sp[n] == "-l") {
	args.local_name=sp[++n];
    }
    else if (sp[n] == "-m") {
	args.member_name=sp[++n];
    }
    else if (sp[n] == "-I") {
	args.inventoryOnly=true;
	args.updateDB=false;
    }
    else if (sp[n] == "-R") {
	args.regenerate=false;
    }
    else if (sp[n] == "-S") {
	args.updateSummary=false;
    }
  }
  if (args.format.length() == 0) {
    std::cerr << "Error: no format specified" << std::endl;
    exit(1);
  }
  else {
    args.format=strutils::to_lower(args.format);
  }
  if (args.dsnum.length() == 0) {
    std::cerr << "Error: no dataset number specified" << std::endl;
    exit(1);
  }
  if (args.dsnum == "999.9") {
    args.overridePrimaryCheck=true;
    args.updateDB=false;
    args.updateSummary=false;
    args.regenerate=false;
  }
  args.path=sp[sp.size()-1];
  auto idx=args.path.length()-1;
  while (idx > 0 && args.path[idx] != '/') {
    idx--;
  }
  args.filename=args.path.substr(idx+1);
  args.path=args.path.substr(0,idx);
}

extern "C" void segv_handler(int)
{
  cleanUp();
  metautils::cmd_unregister();
  metautils::logError("core dump","hdf2xml",user,args.argsString);
}

extern "C" void int_handler(int)
{
  cleanUp();
  metautils::cmd_unregister();
}

void gridInitialize()
{
  if (grid_table == nullptr) {
    grid_table=new my::map<metadata::GrML::GridEntry>;
    gentry=new metadata::GrML::GridEntry;
    lentry=new metadata::GrML::LevelEntry;
    param_entry=new metadata::GrML::ParameterEntry;
  }
}

void gridFinalize()
{
  delete grid_table;
  delete gentry;
  delete lentry;
  delete param_entry;
}

void obsInitialize()
{
  if (ID_table == nullptr) {
    ID_table=new my::map<metadata::ObML::IDEntry> *[metadata::ObML::NUM_OBS_TYPES];
    for (size_t n=0; n < metadata::ObML::NUM_OBS_TYPES; n++) {
	ID_table[n]=new my::map<metadata::ObML::IDEntry>(9999);
    }
    platform_table=new my::map<metadata::ObML::PlatformEntry>[metadata::ObML::NUM_OBS_TYPES];
  }
}

void obsFinalize()
{
  for (size_t n=0; n < metadata::ObML::NUM_OBS_TYPES; n++) {
    delete ID_table[n];
  }
  delete[] ID_table;
  delete[] platform_table;
}

void scanQuikSCATHDF4File(InputHDF4Stream& istream)
{
istream.printDataDescriptors(1965);
}

void scanHDF4File(std::list<std::string>& filelist,ScanData& scan_data)
{
  InputHDF4Stream istream;

  for (const auto& file : filelist) {
    if (!istream.open(file.c_str())) {
	myerror+=" - file: '"+file+"'";
	exit(1);
    }
    if (args.format == "quikscathdf4") {
	scanQuikSCATHDF4File(istream);
    }
    istream.close();
  }
}

struct LibEntry {
  struct Data {
    Data() : ID(),ISPD_ID(),lat(0.),lon(0.),plat_type(0),isrc(0),csrc(' '),already_counted(false) {}

    std::string ID,ISPD_ID;
    float lat,lon;
    short plat_type,isrc;
    char csrc;
    bool already_counted;
  };

  LibEntry() : key(),data(nullptr) {}

  std::string key;
  std::shared_ptr<Data> data;
};

std::string getISPDHDF5PlatformType(const LibEntry& le)
{
  if (le.data->plat_type == -1) {
    return "land_station";
  }
  else {
    switch (le.data->plat_type) {
	case 0:
	case 1:
	case 5:
	case 2002:
	  return "roving_ship";
	case 2:
	case 3:
	case 1007:
	  return "ocean_station";
	case 4:
	  return "lightship";
	case 6:
	  return "moored_buoy";
	case 7:
	case 1009:
	case 2007:
	  if (le.data->ISPD_ID == "008000" || le.data->ISPD_ID == "008001") {
	    return "unknown";
	  }
	  else {
	    return "drifting_buoy";
	  }
	case 9:
	  return "ice_station";
	case 10:
	case 11:
	case 12:
	case 17:
	case 18:
	case 19:
	case 20:
	case 21:
	  return "oceanographic";
	case 13:
	  return "CMAN_station";
	case 1001:
	case 1002:
	case 2001:
	case 2003:
	case 2004:
	case 2005:
	case 2010:
	case 2011:
	case 2012:
	case 2013:
	case 2020:
	case 2021:
	case 2022:
	case 2023:
	case 2024:
	case 2025:
	case 2031:
	case 2040:
	  return "land_station";
	case 14:
	  return "coastal_station";
	case 15:
	  return "fixed_ocean_platform";
	case 2030:
	  return "bogus";
	case 1003:
	case 1006:
	  if ((le.data->ISPD_ID == "001000" && ((le.data->csrc >= '2' && le.data->csrc <= '5') || (le.data->csrc >= 'A' && le.data->csrc <= 'H') || le.data->csrc == 'N')) || le.data->ISPD_ID == "001003" || (le.data->ISPD_ID == "001005" && le.data->plat_type == 1003) || le.data->ISPD_ID == "003002" || le.data->ISPD_ID == "003004" || le.data->ISPD_ID == "003005" || le.data->ISPD_ID == "003006" || le.data->ISPD_ID == "003007" || le.data->ISPD_ID == "003007" || le.data->ISPD_ID == "003008" || le.data->ISPD_ID == "003009" || le.data->ISPD_ID == "003011" || le.data->ISPD_ID == "003014" || le.data->ISPD_ID == "003015" || le.data->ISPD_ID == "003021" || le.data->ISPD_ID == "003022" || le.data->ISPD_ID == "003026" || le.data->ISPD_ID == "004000" || le.data->ISPD_ID == "004003") {
	    return "land_station";
	  }
	  else if (le.data->ISPD_ID == "002000") {
	    if (le.data->ID.length() == 5) {
		if (std::stoi(le.data->ID) < 99000) {
		  return "land_station";
		}
		else if (std::stoi(le.data->ID) < 99100) {
		  return "fixed_ship";
		}
		else {
		  return "roving_ship";
		}
	    }
	    else {
		return "unknown";
	    }
	  }
	  else if (le.data->ISPD_ID == "002001" || (le.data->ISPD_ID == "001005" && le.data->plat_type == 1006)) {
		return "unknown";
	  }
	  else if (le.data->ISPD_ID == "003010") {
	    std::deque<std::string> sp=strutils::split(le.data->ID,"-");
	    if (sp.size() == 2 && sp[1].length() == 5 && strutils::is_numeric(sp[1])) {
		return "land_station";
	    }
	  }
	  else if (le.data->ISPD_ID >= "010000" && le.data->ISPD_ID <= "019999") {
	    if (le.data->plat_type == 1006 && le.data->ID.length() == 5 && strutils::is_numeric(le.data->ID)) {
		if (le.data->ID < "99000") {
		  return "land_station";
		}
		else if (le.data->ID >= "99200" && le.data->ID <= "99299") {
		  return "drifting_buoy";
		}
	    }
	    else {
		return "unknown";
	    }
	  }
	  else {
//	    metautils::logWarning("unknown platform type (1) for station '"+le.data->ID+"' "+strutils::ftos(le.data->lat,4)+" "+strutils::ftos(le.data->lon,4)+" "+le.data->ISPD_ID+" "+strutils::itos(le.data->plat_type)+" "+strutils::itos(le.data->isrc)+" '"+std::string(1,le.data->csrc)+"'","hdf2xml",user,args.argsString);
wss << "unknown platform type (1) for station '"+le.data->ID+"' "+strutils::ftos(le.data->lat,4)+" "+strutils::ftos(le.data->lon,4)+" "+le.data->ISPD_ID+" "+strutils::itos(le.data->plat_type)+" "+strutils::itos(le.data->isrc)+" '"+std::string(1,le.data->csrc)+"'" << std::endl;
	    return "";
	  }
	default:
//	  metautils::logWarning("unknown platform type (2) for station '"+le.data->ID+"' "+strutils::ftos(le.data->lat,4)+" "+strutils::ftos(le.data->lon,4)+" "+le.data->ISPD_ID+" "+strutils::itos(le.data->plat_type)+" "+strutils::itos(le.data->isrc)+" '"+std::string(1,le.data->csrc)+"'","hdf2xml",user,args.argsString);
wss << "unknown platform type (2) for station '"+le.data->ID+"' "+strutils::ftos(le.data->lat,4)+" "+strutils::ftos(le.data->lon,4)+" "+le.data->ISPD_ID+" "+strutils::itos(le.data->plat_type)+" "+strutils::itos(le.data->isrc)+" '"+std::string(1,le.data->csrc)+"'" << std::endl;
	  return "";
    }
  }
}

std::string getISPDHDF5IDEntry(LibEntry& le,std::string pentry_key,DateTime& dt)
{
  std::deque<std::string> sp;
  std::string ientry_key;

  ientry_key="";
  if (le.data->isrc > 0 && le.data->ID.length() > 0 && (le.data->ID)[1] == ' ') {
    sp=strutils::split(le.data->ID);
    ientry_key=pentry_key+"[!]";
    switch (std::stoi(sp[0])) {
	case 2:
	  ientry_key+="generic[!]"+sp[1];
	  break;
	case 3:
	  ientry_key+="WMO[!]"+sp[1];
	  break;
	case 5:
	  ientry_key+="NDBC[!]"+sp[1];
	  break;
	default:
	  ientry_key+="[!]"+le.data->ID;
    }
  }
  else if (le.data->ISPD_ID == "001000") {
    if ((le.data->ID)[6] == '-') {
	sp=strutils::split(le.data->ID,"-");
	if (sp[0] != "999999") {
	  ientry_key=pentry_key+"[!]WMO+6[!]"+sp[0];
	}
	else {
	  if (sp[1] != "99999") {
	    ientry_key=pentry_key+"[!]WBAN[!]"+sp[1];
	  }
	  else {
//	    metautils::logWarning("unknown ID type (1) for station '"+le.data->ID+"' "+strutils::ftos(le.data->lat,4)+" "+strutils::ftos(le.data->lon,4)+" "+le.data->ISPD_ID+" "+strutils::itos(le.data->plat_type)+" "+strutils::itos(le.data->isrc)+" '"+std::string(1,le.data->csrc)+"'","hdf2xml",user,args.argsString);
wss << "unknown ID type (1) for station '"+le.data->ID+"' "+strutils::ftos(le.data->lat,4)+" "+strutils::ftos(le.data->lon,4)+" "+le.data->ISPD_ID+" "+strutils::itos(le.data->plat_type)+" "+strutils::itos(le.data->isrc)+" '"+std::string(1,le.data->csrc)+"'" << std::endl;
	  }
	}
    }
    else {
//	metautils::logWarning("unknown ID type (2) for station '"+le.data->ID+"' "+strutils::ftos(le.data->lat,4)+" "+strutils::ftos(le.data->lon,4)+" "+le.data->ISPD_ID+" "+strutils::itos(le.data->plat_type)+" "+strutils::itos(le.data->isrc)+" '"+std::string(1,le.data->csrc)+"'","hdf2xml",user,args.argsString);
wss << "unknown ID type (2) for station '"+le.data->ID+"' "+strutils::ftos(le.data->lat,4)+" "+strutils::ftos(le.data->lon,4)+" "+le.data->ISPD_ID+" "+strutils::itos(le.data->plat_type)+" "+strutils::itos(le.data->isrc)+" '"+std::string(1,le.data->csrc)+"'" << std::endl;
    }
  }
  else if (le.data->ISPD_ID == "001002") {
    ientry_key=pentry_key+"[!]WBAN[!]"+le.data->ID;
  }
  else if (le.data->ISPD_ID == "001003") {
    ientry_key=pentry_key+"[!]RUSSIA[!]"+le.data->ID;
  }
  else if (le.data->ISPD_ID == "001005" || le.data->ISPD_ID == "001006") {
    if (le.data->plat_type >= 1001 && le.data->plat_type <= 1003 && strutils::is_numeric(le.data->ID)) {
	if (le.data->ID.length() == 5) {
	  ientry_key=pentry_key+"[!]WMO[!]"+le.data->ID;
	}
	else if (le.data->ID.length() == 6) {
	  ientry_key=pentry_key+"[!]WMO+6[!]"+le.data->ID;
	}
    }
    else if (le.data->plat_type == 1002 && !strutils::is_numeric(le.data->ID)) {
	ientry_key=pentry_key+"[!]NAME[!]"+le.data->ID;
    }
    else if (le.data->ID == "999999999999") {
	ientry_key=pentry_key+"[!]unknown[!]"+le.data->ID;
    }
  }
  else if ((le.data->ISPD_ID == "001007" && le.data->plat_type == 1001) || le.data->ISPD_ID == "002000" || le.data->ISPD_ID == "003002" || le.data->ISPD_ID == "003008" || le.data->ISPD_ID == "003015" || le.data->ISPD_ID == "004000" || le.data->ISPD_ID == "004001" || le.data->ISPD_ID == "004003") {
    ientry_key=pentry_key+"[!]WMO[!]"+le.data->ID;
  }
  else if (((le.data->ISPD_ID == "001011" && le.data->plat_type == 1002) || le.data->ISPD_ID == "001007" || le.data->ISPD_ID == "004002" || le.data->ISPD_ID == "004004") && !strutils::is_numeric(le.data->ID)) {
    ientry_key=pentry_key+"[!]NAME[!]"+le.data->ID;
  }
  else if (le.data->ISPD_ID == "001012" && le.data->plat_type == 1002) {
    ientry_key=pentry_key+"[!]COOP[!]"+le.data->ID;
  }
  else if (le.data->ISPD_ID == "002001") {
    if (strutils::is_numeric(le.data->ID)) {
	if (le.data->ID.length() == 5) {
	  if (dt.getYear() <= 1948) {
	    ientry_key=pentry_key+"[!]WBAN[!]"+le.data->ID;
	  }
	  else {
	    ientry_key=pentry_key+"[!]WMO[!]"+le.data->ID;
	  }
	}
	else {
	  ientry_key=pentry_key+"[!]unknown[!]"+le.data->ID;
	}
    }
    else {
	ientry_key=pentry_key+"[!]callSign[!]"+le.data->ID;
    }
  }
  else if (le.data->ISPD_ID == "003002" && strutils::is_numeric(le.data->ID)) {
    if (le.data->ID.length() == 5) {
	ientry_key=pentry_key+"[!]WMO[!]"+le.data->ID;
    }
    else if (le.data->ID.length() == 6) {
	ientry_key=pentry_key+"[!]WMO+6[!]"+le.data->ID;
    }
  }
  else if (le.data->ISPD_ID == "003004") {
    ientry_key=pentry_key+"[!]CANADA[!]"+le.data->ID;
  }
  else if ((le.data->ISPD_ID == "003006" || le.data->ISPD_ID == "003030") && le.data->plat_type == 1006) {
    ientry_key=pentry_key+"[!]AUSTRALIA[!]"+le.data->ID;
  }
  else if (le.data->ISPD_ID == "003009" && le.data->plat_type == 1006) {
    ientry_key=pentry_key+"[!]SPAIN[!]"+le.data->ID;
  }
  else if ((le.data->ISPD_ID == "003010" || le.data->ISPD_ID == "003011") && le.data->plat_type == 1003) {
    sp=strutils::split(le.data->ID,"-");
    if (sp.size() == 2 && sp[1].length() == 5 && strutils::is_numeric(sp[1])) {
	ientry_key=pentry_key+"[!]WMO[!]"+le.data->ID;
    }
  }
  else if (le.data->ISPD_ID == "003012" && le.data->plat_type == 1002) {
    ientry_key=pentry_key+"[!]SWITZERLAND[!]"+le.data->ID;
  }
  else if (le.data->ISPD_ID == "003013" && (le.data->plat_type == 1002 || le.data->plat_type == 1003)) {
    ientry_key=pentry_key+"[!]SOUTHAFRICA[!]"+le.data->ID;
  }
  else if (le.data->ISPD_ID == "003014" && le.data->plat_type == 1003) {
    ientry_key=pentry_key+"[!]NORWAY[!]"+le.data->ID;
  }
  else if (le.data->ISPD_ID == "003016" && le.data->plat_type == 1002) {
    ientry_key=pentry_key+"[!]PORTUGAL[!]"+le.data->ID;
  }
  else if ((le.data->ISPD_ID == "003019" || le.data->ISPD_ID == "003100") && le.data->plat_type == 1002 && le.data->ID.length() > 0) {
    ientry_key=pentry_key+"[!]NEWZEALAND[!]"+le.data->ID;
  }
  else if ((le.data->ISPD_ID == "003007" || le.data->ISPD_ID == "003021" || le.data->ISPD_ID == "003022" || le.data->ISPD_ID == "003023" || le.data->ISPD_ID == "003025" || le.data->ISPD_ID == "003101" || le.data->ISPD_ID == "004005" || le.data->ISPD_ID == "006000") && le.data->plat_type == 1002 && le.data->ID.length() > 0) {
    ientry_key=pentry_key+"[!]NAME[!]"+le.data->ID;
  }
  else if (le.data->ISPD_ID == "003026" && le.data->plat_type == 1006 && le.data->ID.length() == 5) {
    ientry_key=pentry_key+"[!]WMO[!]"+le.data->ID;
  }
  else if (le.data->ISPD_ID == "003030" && le.data->plat_type == 2001) {
    if (strutils::is_numeric(le.data->ID)) {
	ientry_key=pentry_key+"[!]AUSTRALIA[!]"+le.data->ID;
    }
    else {
	ientry_key=pentry_key+"[!]unknown[!]"+le.data->ID;
    }
  }
  else if (le.data->ISPD_ID == "008000" || le.data->ISPD_ID == "008001") {
    ientry_key=pentry_key+"[!]TropicalCyclone[!]"+le.data->ID;
  }
  else if (le.data->ISPD_ID >= "010000" && le.data->ISPD_ID <= "019999") {
    if (le.data->ID.length() == 5 && strutils::is_numeric(le.data->ID)) {
	ientry_key=pentry_key+"[!]WMO[!]"+le.data->ID;
    }
    else {
	ientry_key=pentry_key+"[!]unknown[!]"+le.data->ID;
    }
  }
  else if (le.data->ID == "999999999999" || (le.data->ID.length() > 0 && (le.data->ISPD_ID == "001013" || le.data->ISPD_ID == "001014" || le.data->ISPD_ID == "001018" || le.data->ISPD_ID == "003005" || le.data->ISPD_ID == "003020" || le.data->ISPD_ID == "005000"))) {
    ientry_key=pentry_key+"[!]unknown[!]"+le.data->ID;
  }
  if (ientry_key.length() == 0) {
//    metautils::logWarning("unknown ID type (3) for station '"+le.data->ID+"' "+strutils::ftos(le.data->lat,4)+" "+strutils::ftos(le.data->lon,4)+" "+le.data->ISPD_ID+" "+strutils::itos(le.data->plat_type)+" "+strutils::itos(le.data->isrc)+" '"+std::string(1,le.data->csrc)+"'","hdf2xml",user,args.argsString);
wss << "unknown ID type (3) for station '"+le.data->ID+"' "+strutils::ftos(le.data->lat,4)+" "+strutils::ftos(le.data->lon,4)+" "+le.data->ISPD_ID+" "+strutils::itos(le.data->plat_type)+" "+strutils::itos(le.data->isrc)+" '"+std::string(1,le.data->csrc)+"'" << std::endl;
  }
  return ientry_key;
}

void addISPDHDF5ID(metadata::ObML::IDEntry& ientry,LibEntry& le,DateTime& dt,size_t& num_not_missing)
{
  size_t n,m;

  if (!ID_table[1]->found(ientry.key,ientry)) {
    num_not_missing++;
    ientry.data.reset(new metadata::ObML::IDEntry::Data);
    ientry.data->min_lon_bitmap.reset(new float[360]);
    ientry.data->max_lon_bitmap.reset(new float[360]);
    for (m=0; m < 360; ++m) {
	ientry.data->min_lon_bitmap[m]=ientry.data->max_lon_bitmap[m]=999.;
    }
    ientry.data->S_lat=ientry.data->N_lat=le.data->lat;
    ientry.data->W_lon=ientry.data->E_lon=le.data->lon;
    convertLatLonToBox(1,0.,le.data->lon,n,m);
    ientry.data->min_lon_bitmap[m]=ientry.data->max_lon_bitmap[m]=le.data->lon;
    ientry.data->start=dt;
    ientry.data->end=dt;
    ientry.data->nsteps=1;
    ID_table[1]->insert(ientry);
  }
  else {
    ++num_not_missing;
    if (le.data->lat < ientry.data->S_lat) {
	ientry.data->S_lat=le.data->lat;
    }
    if (le.data->lat > ientry.data->N_lat) {
	ientry.data->N_lat=le.data->lat;
    }
    if (le.data->lon < ientry.data->W_lon) {
	ientry.data->W_lon=le.data->lon;
    }
    if (le.data->lon > ientry.data->E_lon) {
	ientry.data->E_lon=le.data->lon;
    }
    convertLatLonToBox(1,0.,le.data->lon,n,m);
    if (ientry.data->min_lon_bitmap[m] > 900.) {
	ientry.data->min_lon_bitmap[m]=ientry.data->max_lon_bitmap[m]=le.data->lon;
    }
    else {
	if (le.data->lon < ientry.data->min_lon_bitmap[m]) {
	  ientry.data->min_lon_bitmap[m]=le.data->lon;
	}
	if (le.data->lon > ientry.data->max_lon_bitmap[m]) {
	  ientry.data->max_lon_bitmap[m]=le.data->lon;
	}
    }
    if (dt < ientry.data->start) {
	ientry.data->start=dt;
    }
    if (dt > ientry.data->end) {
	ientry.data->end=dt;
    }
    if (!le.data->already_counted) {
	++(ientry.data->nsteps);
    }
  }
  le.data->already_counted=true;
}

void addISPDHDF5Platform(metadata::ObML::PlatformEntry& pentry,LibEntry& le)
{
  size_t n,m;

  if (!platform_table[1].found(pentry.key,pentry)) {
    pentry.boxflags.reset(new summarizeMetadata::BoxFlags);
    pentry.boxflags->initialize(361,180,0,0);
    if (le.data->lat == -90.) {
	pentry.boxflags->spole=1;
    }
    else if (le.data->lat == 90.) {
	pentry.boxflags->npole=1;
    }
    else {
	convertLatLonToBox(1,le.data->lat,le.data->lon,n,m);
	pentry.boxflags->flags[n-1][m]=1;
	pentry.boxflags->flags[n-1][360]=1;
    }
    platform_table[1].insert(pentry);
  }
  else {
    if (le.data->lat == -90.) {
	pentry.boxflags->spole=1;
    }
    else if (le.data->lat == 90.) {
	pentry.boxflags->npole=1;
    }
    else {
	convertLatLonToBox(1,le.data->lat,le.data->lon,n,m);
	pentry.boxflags->flags[n-1][m]=1;
	pentry.boxflags->flags[n-1][360]=1;
    }
  }
}

void scanISPDHDF5File(InputHDF5Stream& istream)
{
  InputHDF5Stream::Dataset *ds;
  InputHDF5Stream::CompoundDatatype cpd;
  int m,l;
  InputHDF5Stream::DataValue dv;
  my::map<LibEntry> stn_library(9999);
  LibEntry le;
  metadata::ObML::IDEntry ientry;
  metadata::ObML::PlatformEntry pentry;
  std::string timestamp,sdum;
  DateTime dt;
  double v[3]={0.,0.,0.};
  metadata::ObML::DataTypeEntry de;

  obsInitialize();
// load the station library
  if ( (ds=istream.getDataset("/Data/SpatialTemporalLocation/SpatialTemporalLocation")) == NULL || ds->datatype.class_ != 6) {
    myerror="unable to locate spatial/temporal information";
    exit(1);
  }
  HDF5::decodeCompoundDatatype(ds->datatype,cpd,istream.debugIsOn());
  for (const auto& chunk : ds->data.chunks) {
    for (m=0,l=0; m < ds->data.sizes.front(); m++) {
	dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[0].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[0].datatype,ds->dataspace,istream.debugIsOn());
	le.key=reinterpret_cast<char *>(dv.get());
	dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[1].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[1].datatype,ds->dataspace,istream.debugIsOn());
	le.key+=std::string(reinterpret_cast<char *>(dv.get()));
	if (le.key.length() > 0 && !stn_library.found(le.key,le)) {
	  le.data.reset(new LibEntry::Data);
	  le.data->plat_type=-1;
	  le.data->isrc=-1;
	  le.data->csrc='9';
	  le.data->already_counted=false;
	  dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[2].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[2].datatype,ds->dataspace,istream.debugIsOn());
	  le.data->ID=reinterpret_cast<char *>(dv.get());
	  strutils::trim(le.data->ID);
	  dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[13].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[13].datatype,ds->dataspace,istream.debugIsOn());
	  le.data->lat=*(reinterpret_cast<float *>(dv.get()));
	  dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[14].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[14].datatype,ds->dataspace,istream.debugIsOn());
	  le.data->lon=*(reinterpret_cast<float *>(dv.get()));
	  if (le.data->lon > 180.) {
	    le.data->lon-=360.;
	  }
	  stn_library.insert(le);
	}
	l+=ds->data.size_of_element;
    }
  }
// load the ICOADS platform types
  if ( (ds=istream.getDataset("/SupplementalData/Tracking/ICOADS/TrackingICOADS")) != NULL && ds->datatype.class_ == 6) {
    HDF5::decodeCompoundDatatype(ds->datatype,cpd,istream.debugIsOn());
    for (const auto& chunk : ds->data.chunks) {
	for (m=0,l=0; m < ds->data.sizes.front(); ++m) {
	  dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[0].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[0].datatype,ds->dataspace,istream.debugIsOn());
	  le.key=reinterpret_cast<char *>(dv.get());
	  if (le.key.length() > 0) {
	    dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[1].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[1].datatype,ds->dataspace,istream.debugIsOn());
	    le.key+=std::string(reinterpret_cast<char *>(dv.get()));
	    if (stn_library.found(le.key,le)) {
		dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[2].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[2].datatype,ds->dataspace,istream.debugIsOn());
		le.data->isrc=*(reinterpret_cast<int *>(dv.get()));
		dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[4].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[4].datatype,ds->dataspace,istream.debugIsOn());
		le.data->plat_type=*(reinterpret_cast<int *>(dv.get()));
	    }
	    else {
		metautils::logError("no entry for '"+le.key+"' in station library","hdf2xml",user,args.argsString);
	    }
	  }
	  l+=ds->data.size_of_element;
	}
    }
  }
// load observation types for IDs that don't already have a platform type
  if ( (ds=istream.getDataset("/Data/Observations/ObservationTypes")) != NULL && ds->datatype.class_ == 6) {
    HDF5::decodeCompoundDatatype(ds->datatype,cpd,istream.debugIsOn());
    for (const auto& chunk : ds->data.chunks) {
	for (m=0,l=0; m < ds->data.sizes.front(); ++m) {
	  dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[0].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[0].datatype,ds->dataspace,istream.debugIsOn());
	  le.key=reinterpret_cast<char *>(dv.get());
	  if (le.key.length() > 0) {
	    dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[1].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[1].datatype,ds->dataspace,istream.debugIsOn());
	    le.key+=std::string(reinterpret_cast<char *>(dv.get()));
	    if (stn_library.found(le.key,le)) {
		if (le.data->plat_type < 0) {
		  dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[2].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[2].datatype,ds->dataspace,istream.debugIsOn());
		  le.data->plat_type=1000+*(reinterpret_cast<int *>(dv.get()));
		}
		dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[4].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[4].datatype,ds->dataspace,istream.debugIsOn());
		le.data->ISPD_ID=reinterpret_cast<char *>(dv.get());
		strutils::replace_all(le.data->ISPD_ID," ","0");
	    }
	    else {
		metautils::logError("no entry for '"+le.key+"' in station library","hdf2xml",user,args.argsString);
	    }
	  }
	  l+=ds->data.size_of_element;
	}
    }
  }
  if ( (ds=istream.getDataset("/SupplementalData/Tracking/Land/TrackingLand")) != NULL && ds->datatype.class_ == 6) {
    HDF5::decodeCompoundDatatype(ds->datatype,cpd,istream.debugIsOn());
    for (const auto& chunk : ds->data.chunks) {
	for (m=0,l=0; m < ds->data.sizes.front(); ++m) {
	  dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[0].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[0].datatype,ds->dataspace,istream.debugIsOn());
	  le.key=reinterpret_cast<char *>(dv.get());
	  if (le.key.length() > 0) {
	    dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[1].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[1].datatype,ds->dataspace,istream.debugIsOn());
	    le.key+=std::string(reinterpret_cast<char *>(dv.get()));
	    if (stn_library.found(le.key,le)) {
		dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[2].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[2].datatype,ds->dataspace,istream.debugIsOn());
		le.data->csrc=(reinterpret_cast<char *>(dv.get()))[0];
		dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[3].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[3].datatype,ds->dataspace,istream.debugIsOn());
		sdum=reinterpret_cast<char *>(dv.get());
		if (sdum == "FM-12") {
		  le.data->plat_type=2001;
		}
		else if (sdum == "FM-13") {
		  le.data->plat_type=2002;
		}
		else if (sdum == "FM-14") {
		  le.data->plat_type=2003;
		}
		else if (sdum == "FM-15") {
		  le.data->plat_type=2004;
		}
		else if (sdum == "FM-16") {
		  le.data->plat_type=2005;
		}
		else if (sdum == "FM-18") {
		  le.data->plat_type=2007;
		}
		else if (sdum == "  SAO") {
		  le.data->plat_type=2010;
		}
		else if (sdum == " AOSP") {
		  le.data->plat_type=2011;
		}
		else if (sdum == " AERO") {
		  le.data->plat_type=2012;
		}
		else if (sdum == " AUTO") {
		  le.data->plat_type=2013;
		}
		else if (sdum == "SY-AE") {
		  le.data->plat_type=2020;
		}
		else if (sdum == "SY-SA") {
		  le.data->plat_type=2021;
		}
		else if (sdum == "SY-MT") {
		  le.data->plat_type=2022;
		}
		else if (sdum == "SY-AU") {
		  le.data->plat_type=2023;
		}
		else if (sdum == "SA-AU") {
		  le.data->plat_type=2024;
		}
		else if (sdum == "S-S-A") {
		  le.data->plat_type=2025;
		}
		else if (sdum == "BOGUS") {
		  le.data->plat_type=2030;
		}
		else if (sdum == "SMARS") {
		  le.data->plat_type=2031;
		}
		else if (sdum == "  SOD") {
		  le.data->plat_type=2040;
		}
	    }
	    else {
		metautils::logError("no entry for '"+le.key+"' in station library","hdf2xml",user,args.argsString);
	    }
	  }
	  l+=ds->data.size_of_element;
	}
    }
  }
// load tropical storm IDs
  if ( (ds=istream.getDataset("/SupplementalData/Misc/TropicalStorms/StormID")) != NULL && ds->datatype.class_ == 6) {
    HDF5::decodeCompoundDatatype(ds->datatype,cpd,istream.debugIsOn());
    for (const auto& chunk : ds->data.chunks) {
	for (m=0,l=0; m < ds->data.sizes.front(); ++m) {
	  dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[0].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[0].datatype,ds->dataspace,istream.debugIsOn());
	  le.key=reinterpret_cast<char *>(dv.get());
	  if (le.key.length() > 0) {
	    dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[1].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[1].datatype,ds->dataspace,istream.debugIsOn());
	    le.key+=std::string(reinterpret_cast<char *>(dv.get()));
	    if (stn_library.found(le.key,le)) {
		dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[2].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[2].datatype,ds->dataspace,istream.debugIsOn());
		le.data->ID=reinterpret_cast<char *>(dv.get());
		strutils::trim(le.data->ID);
	    }
	    else {
		metautils::logError("no entry for '"+le.key+"' in station library","hdf2xml",user,args.argsString);
	    }
	  }
	  l+=ds->data.size_of_element;
	}
    }
  }
// scan the observations
  if ( (ds=istream.getDataset("/Data/Observations/Observations")) == NULL || ds->datatype.class_ != 6) {
    myerror="unable to locate observations";
    exit(1);
  }
  HDF5::decodeCompoundDatatype(ds->datatype,cpd,istream.debugIsOn());
  for (const auto& chunk : ds->data.chunks) {
    for (m=0,l=0; m < ds->data.sizes.front(); ++m) {
	dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[0].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[0].datatype,ds->dataspace,istream.debugIsOn());
	timestamp=reinterpret_cast<char *>(dv.get());
	strutils::trim(timestamp);
	if (timestamp.length() > 0) {
	  le.key=timestamp;
	  dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[1].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[1].datatype,ds->dataspace,istream.debugIsOn());
	  le.key+=std::string(reinterpret_cast<char *>(dv.get()));
	  if (stn_library.found(le.key,le)) {
	    if (timestamp.length() > 0) {
		if (strutils::has_ending(timestamp,"99")) {
		  strutils::chop(timestamp,2);
// patch for some bad timestamps
		  if (strutils::has_ending(timestamp," ")) {
		    strutils::chop(timestamp);
		    timestamp.insert(8,"0");
		  }
		  timestamp+="00";
		}
		dt.set(std::stoll(timestamp)*100);
		pentry.key=getISPDHDF5PlatformType(le);
		if (pentry.key.length() > 0) {
		  ientry.key=getISPDHDF5IDEntry(le,pentry.key,dt);
		  if (ientry.key.length() > 0) {
// SLP
		    dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[2].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[2].datatype,ds->dataspace,istream.debugIsOn());
		    if (dv.class_ != 1) {
			metautils::logError("observed SLP is not a floating point number for '"+ientry.key+"'","hdf2xml",user,args.argsString);
		    }
		    if (dv.precision == 32) {
			v[0]=*(reinterpret_cast<float *>(dv.get()));
		    }
		    else if (dv.precision == 64) {
			v[0]=*(reinterpret_cast<double *>(dv.get()));
		    }
		    else {
			metautils::logError("bad precision ("+strutils::itos(dv.precision)+") for SLP","hdf2xml",user,args.argsString);
		    }
// STN P
		    dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[5].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[5].datatype,ds->dataspace,istream.debugIsOn());
		    if (dv.class_ != 1) {
			metautils::logError("observed STN P is not a floating point number for '"+ientry.key+"'","hdf2xml",user,args.argsString);
		    }
		    if (dv.precision == 32) {
			v[1]=*(reinterpret_cast<float *>(dv.get()));
		    }
		    else if (dv.precision == 64) {
			v[1]=*(reinterpret_cast<double *>(dv.get()));
		    }
		    else {
			metautils::logError("bad precision ("+strutils::itos(dv.precision)+") for SLP","hdf2xml",user,args.argsString);
		    }
		    if ((v[0] >= 860. && v[0] <= 1090.) || (v[1] >= 400. && v[1] <= 1090.)) {
			addISPDHDF5ID(ientry,le,dt,num_not_missing);
			if (v[0] < 9999.9) {
			  de.key="SLP";
			  if (!ientry.data->data_types_table.found(de.key,de)) {
			    de.data.reset(new metadata::ObML::DataTypeEntry::Data);
			    de.data->nsteps=1;
			    ientry.data->data_types_table.insert(de);
			  }
			  else {
			    ++(de.data->nsteps);
			  }
			}
			if (v[1] < 9999.9) {
			  de.key="STNP";
			  if (!ientry.data->data_types_table.found(de.key,de)) {
			    de.data.reset(new metadata::ObML::DataTypeEntry::Data);
			    de.data->nsteps=1;
			    ientry.data->data_types_table.insert(de);
			  }
			  else {
			    ++(de.data->nsteps);
			  }
			}
			addISPDHDF5Platform(pentry,le);
		    }
		  }
		}
	    }
	  }
	  else {
	    metautils::logError("no entry for '"+le.key+"' in station library","hdf2xml",user,args.argsString);
	  }
	}
	l+=ds->data.size_of_element;
    }
  }
// scan for feedback information
  if ( (ds=istream.getDataset("/Data/AssimilationFeedback/AssimilationFeedback")) == NULL)
    ds=istream.getDataset("/Data/AssimilationFeedback/AssimilationFeedBack");
  if (ds != NULL && ds->datatype.class_ == 6) {
    HDF5::decodeCompoundDatatype(ds->datatype,cpd,istream.debugIsOn());
    for (const auto& chunk : ds->data.chunks) {
	for (m=0,l=0; m < ds->data.sizes.front(); ++m) {
	  dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[0].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[0].datatype,ds->dataspace,istream.debugIsOn());
	  timestamp=reinterpret_cast<char *>(dv.get());
	  strutils::trim(timestamp);
	  if (timestamp.length() > 0) {
	    le.key=timestamp;
	    dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[1].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfOffsets(),cpd.members[1].datatype,ds->dataspace,istream.debugIsOn());
	    le.key+=std::string(reinterpret_cast<char *>(dv.get()));
	    if (stn_library.found(le.key,le)) {
		if (timestamp.length() > 0) {
		  if (strutils::has_ending(timestamp,"99")) {
		    strutils::chop(timestamp,2);
		    timestamp+="00";
		  }
		  dt.set(std::stoll(timestamp)*100);
		  pentry.key=getISPDHDF5PlatformType(le);
		  if (pentry.key.length() > 0) {
		    ientry.key=getISPDHDF5IDEntry(le,pentry.key,dt);
		    if (ientry.key.length() > 0) {
			dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[2].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[2].datatype,ds->dataspace,istream.debugIsOn());
			if (dv.class_ != 1) {
			  metautils::logError("modified observed pressure is not a floating point number for '"+ientry.key+"'","hdf2xml",user,args.argsString);
			}
			if (dv.precision == 32) {
			  v[0]=*(reinterpret_cast<float *>(dv.get()));
			}
			else if (dv.precision == 64) {
			  v[0]=*(reinterpret_cast<double *>(dv.get()));
			}
			else {
			  metautils::logError("bad precision ("+strutils::itos(dv.precision)+") for modified observed pressure","hdf2xml",user,args.argsString);
			}
			dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[11].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[11].datatype,ds->dataspace,istream.debugIsOn());
			if (dv.class_ != 1) {
			  metautils::logError("ensemble first guess pressure is not a floating point number for '"+ientry.key+"'","hdf2xml",user,args.argsString);
			}
			if (dv.precision == 32) {
			  v[1]=*(reinterpret_cast<float *>(dv.get()));
			}
			else if (dv.precision == 64) {
			  v[1]=*(reinterpret_cast<double *>(dv.get()));
			}
			else {
			  metautils::logError("bad precision ("+strutils::itos(dv.precision)+") for ensemble first guess pressure","hdf2xml",user,args.argsString);
			}
			dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[14].byte_offset],istream.getSizeOfOffsets(),istream.getSizeOfLengths(),cpd.members[14].datatype,ds->dataspace,istream.debugIsOn());
			if (dv.class_ != 1) {
			  metautils::logError("ensemble analysis pressure is not a floating point number for '"+ientry.key+"'","hdf2xml",user,args.argsString);
			}
			if (dv.precision == 32) {
			  v[2]=*(reinterpret_cast<float *>(dv.get()));
			}
			else if (dv.precision == 64) {
			  v[2]=*(reinterpret_cast<double *>(dv.get()));
			}
			else {
			  metautils::logError("bad precision ("+strutils::itos(dv.precision)+") for ensemble analysis pressure","hdf2xml",user,args.argsString);
			}
			if ((v[0] >= 400. && v[0] <= 1090.) || (v[1] >= 400. && v[1] <= 1090.) || (v[2] >= 400. && v[2] <= 1090.)) {
			  addISPDHDF5ID(ientry,le,dt,num_not_missing);
			  de.key="Feedback";
			  if (!ientry.data->data_types_table.found(de.key,de)) {
			    de.data.reset(new metadata::ObML::DataTypeEntry::Data);
			    de.data->nsteps=1;
			    ientry.data->data_types_table.insert(de);
			  }
			  else {
			    ++(de.data->nsteps);
			  }
			  addISPDHDF5Platform(pentry,le);
			}
		    }
		  }
		}
	    }
	  }
	  l+=ds->data.size_of_element;
	}
    }
  }
  for (const auto& key : stn_library.keys()) {
    stn_library.found(key,le);
    le.data=nullptr;
  }
  write_type=ObML_type;
}

void scanUSArrayTransportableHDF5File(InputHDF5Stream& istream,ScanData& scan_data)
{
  obsInitialize();
// load the pressure dataset
  InputHDF5Stream::Dataset *ds;
  if ( (ds=istream.getDataset("/obsdata/presdata")) == nullptr || ds->datatype.class_ != 6) {
    myerror="unable to locate the pressure dataset";
    exit(1);
  }
  HDF5::DataArray times,stnids,pres;
  times.fill(istream,*ds,0);
  if (times.type != HDF5::DataArray::long_long_) {
     metautils::logError("expected the timestamps to be 'long long' but got "+strutils::itos(times.type),"hdf2xml",user,args.argsString);
  }
  stnids.fill(istream,*ds,1);
  if (stnids.type != HDF5::DataArray::short_) {
     metautils::logError("expected the numeric station IDs to be 'short' but got "+strutils::itos(stnids.type),"hdf2xml",user,args.argsString);
  }
  pres.fill(istream,*ds,2);
  if (pres.type != HDF5::DataArray::float_) {
     metautils::logError("expected the pressures to be 'float' but got "+strutils::itos(pres.type),"hdf2xml",user,args.argsString);
  }
  int num_values=0;
  float pres_miss_val=3.e48;
  short nID=-1;
  metadata::ObML::IDEntry ientry;
  metadata::ObML::PlatformEntry pentry;
  metadata::ObML::DataTypeEntry de;
  metautils::StringEntry se;
  for (const auto& key : ds->attributes.keys()) {
    InputHDF5Stream::Attribute attr;
    ds->attributes.found(key,attr);
    if (key == "NROWS") {
	num_values=*(reinterpret_cast<int *>(attr.value.value));
    }
    else if (key == "LATITUDE_DDEG") {
	if (ID_table[1]->size() == 1) {
	  ientry.data->S_lat=ientry.data->N_lat=*(reinterpret_cast<float *>(attr.value.value));
	  if (ientry.data->S_lat == -90.) {
	    pentry.boxflags->spole=1;
	  }
	  else if (ientry.data->S_lat == 90.) {
	    pentry.boxflags->npole=1;
	  }
	}
	else {
	  metautils::logError("found latitude but no station ID","hdf2xml",user,args.argsString);
	}
    }
    else if (key == "LONGITUDE_DDEG") {
	if (ID_table[1]->size() == 1) {
	  ientry.data->W_lon=ientry.data->E_lon=*(reinterpret_cast<float *>(attr.value.value));
	  size_t n,m;
	  convertLatLonToBox(1,ientry.data->S_lat,ientry.data->W_lon,n,m);
	  ientry.data->min_lon_bitmap[m]=ientry.data->max_lon_bitmap[m]=ientry.data->W_lon;
	  if (pentry.boxflags->spole == 0 && pentry.boxflags->npole == 0) {
	    pentry.boxflags->flags[n-1][m]=1;
	    pentry.boxflags->flags[n-1][360]=1;
	  }
	}
	else {
	  metautils::logError("found longitude but no station ID","hdf2xml",user,args.argsString);
	}
    }
    else if (key == "CHAR_STATION_ID") {
	if (ID_table[1]->size() == 0 && platform_table[1].size() == 0) {
	  num_not_missing++;
	  pentry.key="land_station";
	  ientry.key.assign(reinterpret_cast<char *>(attr.value.value));
	  ientry.key.insert(0,pentry.key+"[!]USArray[!]TA.");
	  ientry.data.reset(new metadata::ObML::IDEntry::Data);
	  ientry.data->min_lon_bitmap.reset(new float[360]);
	  ientry.data->max_lon_bitmap.reset(new float[360]);
	  for (auto m=0; m < 360; ++m) {
	    ientry.data->min_lon_bitmap[m]=ientry.data->max_lon_bitmap[m]=999.;
	  }
	  ientry.data->start=DateTime(3000,12,31,235959,0);
	  ientry.data->end=DateTime(1000,1,1,0,0);
	  ientry.data->nsteps=0;
	  ID_table[1]->insert(ientry);
	  pentry.boxflags.reset(new summarizeMetadata::BoxFlags);
	  pentry.boxflags->initialize(361,180,0,0);
	  platform_table[1].insert(pentry);
	}
	else {
	  metautils::logError("multiple station IDs not expected","hdf2xml",user,args.argsString);
	}
    }
    else if (key == "NUMERIC_STATION_ID") {
	nID=*(reinterpret_cast<short *>(attr.value.value));
    }
    else if (key == "FIELD_2_NAME") {
	de.key.assign(reinterpret_cast<char *>(attr.value.value));
    }
    else if (key == "FIELD_2_FILL") {
	pres_miss_val=*(reinterpret_cast<float *>(attr.value.value));
    }
    else if (key == "FIELD_2_DESCRIPTION") {
	se.key.assign(reinterpret_cast<char *>(attr.value.value));
    }
  }
  if (se.key.length() == 0) {
    metautils::logError("unable to get title for the data value","hdf2xml",user,args.argsString);
  }
  if (de.key.length() == 0) {
    metautils::logError("unable to get the name of the data value","hdf2xml",user,args.argsString);
  }
  else {
    de.data.reset(new metadata::ObML::DataTypeEntry::Data);
    de.data->nsteps=0;
    ientry.data->data_types_table.insert(de);
  }
  DateTime epoch(1970,1,1,0,0);
  for (auto n=0; n < num_values; ++n) {
    if (stnids.short_value(n) != nID) {
	metautils::logError("unexpected change in the numeric station ID","hdf2xml",user,args.argsString);
    }
    if (pres.float_value(n) != pres_miss_val) {
	++(ientry.data->nsteps);
	++(de.data->nsteps);
	DateTime dt=epoch.secondsAdded(times.long_long_value(n));
	if (dt < ientry.data->start) {
	  ientry.data->start=dt;
	}
	if (dt > ientry.data->end) {
	  ientry.data->end=dt;
	}
    }
  }
  scan_data.map_name=getRemoteWebFile("http://rda.ucar.edu/metadata/ParameterTables/HDF5.ds"+args.dsnum+".xml",tdir->name());
  scan_data.found_map=(scan_data.map_name.length() > 0);
  se.key=de.key+"<!>"+se.key+"<!>Hz";
  scan_data.varlist.emplace_back(se.key);
  if (scan_data.found_map) {
    se.key=de.key;
    scan_data.var_changes_table.insert(se);
  }
  write_type=ObML_type;
}

std::string getGriddedTimeMethod(const std::shared_ptr<InputHDF5Stream::Dataset> ds,std::string timeid)
{
  InputHDF5Stream::Attribute attr;
  std::string time_method;

  if (ds->attributes.found("cell_methods",attr) && attr.value.class_ == 3) {
    time_method=metautils::NcTime::getTimeMethodFromCellMethods(reinterpret_cast<char *>(attr.value.value),timeid);
    if (time_method[0] == '!') {
	metautils::logError("cell method '"+time_method.substr(1)+"' is not valid CF","hdf2xml",user,args.argsString);
    }
    else {
	return time_method;
    }
  }
  return "";
}

void addGriddedTimeRange(std::string key_start,my::map<metautils::StringEntry>& gentry_table,const metautils::NcTime::TimeRangeEntry& tre,std::string timeid,InputHDF5Stream& istream)
{
  std::list<InputHDF5Stream::DatasetEntry> vars;
  InputHDF5Stream::Attribute attr;
  std::string gentry_key,time_method,error;
  metautils::StringEntry se;
  InvEntry ie;
  bool found_no_method=false;

  vars=istream.getDatasetsWithAttribute("DIMENSION_LIST");
  for (const auto& var : vars) {
    var.dataset->attributes.found("DIMENSION_LIST",attr);
    if (attr.value.class_ == 9 && attr.value.dim_sizes.size() == 1 && attr.value.dim_sizes[0] > 2) {
	time_method=getGriddedTimeMethod(var.dataset,timeid);
	if (time_method.length() == 0) {
	  found_no_method=true;
	}
	else {
	  ie.key=metautils::NcTime::getGriddedNetCDFTimeRangeDescription(tre,time_data,strutils::capitalize(time_method),error);
	  if (error.length() > 0) {
	    metautils::logError(error,"hdf2xml",user,args.argsString);
	  }
	  gentry_key=key_start+ie.key;
	  if (!gentry_table.found(gentry_key,se)) {
	    se.key=gentry_key;
	    gentry_table.insert(se);
	  }
	}
    }
  }
  if (found_no_method) {
    ie.key=metautils::NcTime::getGriddedNetCDFTimeRangeDescription(tre,time_data,"",error);
    if (error.length() > 0) {
	metautils::logError(error,"hdf2xml",user,args.argsString);
    }
    gentry_key=key_start+ie.key;
    if (!gentry_table.found(gentry_key,se)) {
	se.key=gentry_key;
	gentry_table.insert(se);
    }
  }
  if (inv.is_open() && !inv_U_table.found(ie.key,ie)) {
    ie.num=inv_U_table.size();
    inv_U_table.insert(ie);
  }
}

void addGriddedLatLonKeys(my::map<metautils::StringEntry>& gentry_table,Grid::GridDimensions dim,Grid::GridDefinition def,const metautils::NcTime::TimeRangeEntry& tre,std::string timeid,InputHDF5Stream& istream)
{
  std::string key_start;

  switch (def.type) {
    case Grid::latitudeLongitudeType:
    {
	key_start=strutils::itos(def.type)+"<!>"+strutils::itos(dim.x)+"<!>"+strutils::itos(dim.y)+"<!>"+strutils::ftos(def.slatitude,3)+"<!>"+strutils::ftos(def.slongitude,3)+"<!>"+strutils::ftos(def.elatitude,3)+"<!>"+strutils::ftos(def.elongitude,3)+"<!>"+strutils::ftos(def.loincrement,3)+"<!>"+strutils::ftos(def.laincrement,3)+"<!>";
	addGriddedTimeRange(key_start,gentry_table,tre,timeid,istream);
	break;
    }
    case Grid::polarStereographicType:
    {
	key_start=strutils::itos(def.type)+"<!>"+strutils::itos(dim.x)+"<!>"+strutils::itos(dim.y)+"<!>"+strutils::ftos(def.slatitude,3)+"<!>"+strutils::ftos(def.slongitude,3)+"<!>"+strutils::ftos(def.llatitude,3)+"<!>"+strutils::ftos(def.olongitude,3)+"<!>"+strutils::ftos(def.dx,3)+"<!>"+strutils::ftos(def.dy,3)+"<!>";
	if (def.projection_flag == 0) {
	  key_start+="N";
	}
	else {
	  key_start+="S";
	}
	key_start+="<!>";
	addGriddedTimeRange(key_start,gentry_table,tre,timeid,istream);
	break;
    }
    case Grid::lambertConformalType:
    {
	key_start=strutils::itos(def.type)+"<!>"+strutils::itos(dim.x)+"<!>"+strutils::itos(dim.y)+"<!>"+strutils::ftos(def.slatitude,3)+"<!>"+strutils::ftos(def.slongitude,3)+"<!>"+strutils::ftos(def.llatitude,3)+"<!>"+strutils::ftos(def.olongitude,3)+"<!>"+strutils::ftos(def.dx,3)+"<!>"+strutils::ftos(def.dy,3)+"<!>";
	if (def.projection_flag == 0) {
	  key_start+="N";
	}
	else {
	  key_start+="S";
	}
	key_start+="<!>"+strutils::ftos(def.stdparallel1,3)+"<!>"+strutils::ftos(def.stdparallel2,3)+"<!>";
	addGriddedTimeRange(key_start,gentry_table,tre,timeid,istream);
	break;
    }
  }
  InvEntry ie;
  ie.key=strutils::substitute(key_start,"<!>",",");
  strutils::chop(ie.key);
  if (inv.is_open() && !inv_G_table.found(ie.key,ie)) {
    ie.num=inv_G_table.size();
    inv_G_table.insert(ie);
  }
}

double getValueFromDataArray(const HDF5::DataArray& data_array,size_t index,const InputHDF5Stream::Dataset *ds)
{
  double value=0.;
  switch (ds->datatype.class_) {
    case 0:
    {
	switch (ds->data.size_of_element) {
	  case 4:
	  {
	    value=(reinterpret_cast<int *>(data_array.values))[index];
	    break;
	  }
	  default:
	  {
	    metautils::logError("unable to get value for fixed-point size "+strutils::itos(ds->data.size_of_element),"hdf2xml",user,args.argsString);
	  }
	}
	break;
    }
    case 1:
    {
	switch (ds->data.size_of_element) {
	  case 4:
	  {
	    value=(reinterpret_cast<float *>(data_array.values))[index];
	    break;
	  }
	  case 8:
	  {
	    value=(reinterpret_cast<double *>(data_array.values))[index];
	    break;
	  }
	  default:
	  {
	    metautils::logError("unable to get value for floating-point size "+strutils::itos(ds->data.size_of_element),"hdf2xml",user,args.argsString);
	  }
	}
	break;
    }
    default:
    {
	metautils::logError("unable to decode time from datatype class "+strutils::itos(ds->datatype.class_),"hdf2xml",user,args.argsString);
    }
  }
  return value;
}

void addGriddedNetCDFParameter(const InputHDF5Stream::DatasetEntry& var,ScanData& scan_data,const metautils::NcTime::TimeRange& time_range,ParameterData& parameter_data,int num_steps)
{
  std::string description,units,standard_name,sdum;
  InputHDF5Stream::Attribute attr;
  metautils::StringEntry se;

  description="";
  units="";
  if (var.dataset->attributes.found("units",attr) && attr.value.class_ == 3) {
    units=reinterpret_cast<char *>(attr.value.value);
  }
  if (description.length() == 0) {
    if ((var.dataset->attributes.found("description",attr) || var.dataset->attributes.found("Description",attr)) && attr.value.class_ == 3) {
	description=reinterpret_cast<char *>(attr.value.value);
    }
    else if ((var.dataset->attributes.found("comment",attr) || var.dataset->attributes.found("Comment",attr)) && attr.value.class_ == 3) {
	description=reinterpret_cast<char *>(attr.value.value);
    }
    else if (var.dataset->attributes.found("long_name",attr) && attr.value.class_ == 3) {
	description=reinterpret_cast<char *>(attr.value.value);
    }
  }
  if (var.dataset->attributes.found("standard_name",attr) && attr.value.class_ == 3) {
    standard_name=reinterpret_cast<char *>(attr.value.value);
  }
  sdum=var.key;
  strutils::trim(sdum);
  strutils::trim(description);
  strutils::trim(units);
  strutils::trim(standard_name);
  se.key=sdum+"<!>"+description+"<!>"+units+"<!>"+standard_name;
  if (!parameter_data.table.found(se.key,se)) {
    sdum=parameter_data.map.getShortName(var.key);
    if (!scan_data.found_map || sdum.length() == 0) {
	parameter_data.table.insert(se);
	scan_data.varlist.push_back(se.key);
    }
    else {
	parameter_data.table.insert(se);
	scan_data.varlist.push_back(se.key);
	se.key=var.key;
	scan_data.var_changes_table.insert(se);
    }
  }
  param_entry->startDateTime=time_range.first_valid_datetime;
  param_entry->endDateTime=time_range.last_valid_datetime;
  param_entry->numTimeSteps=num_steps;
  lentry->parameter_code_table.insert(*param_entry);
}

bool parameterMatchesDimensions(InputHDF5Stream& istream,InputHDF5Stream::Attribute& attr,std::string timeid,std::string latid,std::string lonid,std::string levid)
{
  int n,off;
  InputHDF5Stream::ReferenceEntry re[3];
  InputHDF5Stream::Dataset *ds[2]={nullptr,nullptr};
  InputHDF5Stream::Attribute at[2];
  std::stringstream ss[3];
  bool parameter_matches=false;

  off=1+attr.value.precision;
  for (n=1; n < static_cast<int>(attr.value.dim_sizes[0]); n++) {
    if (!istream.getReferenceTablePointer()->found(HDF5::getValue(&(reinterpret_cast<unsigned char *>(attr.value.value))[off],attr.value.precision),re[n-1]))
	metautils::logError("unable to dereference dimension reference","hdf2xml",user,args.argsString);
    off+=attr.value.precision;
  }
  switch (attr.value.dim_sizes[0]) {
    case 3:
	if (levid == "sfc") {
	  if (re[0].name == latid && re[1].name == lonid) {
	    parameter_matches=true;
	  }
	  else if (re[0].name != latid && re[1].name != lonid) {
	    if ( (ds[0]=istream.getDataset("/"+latid)) != nullptr && (ds[1]=istream.getDataset("/"+lonid)) != nullptr) {
		ds[0]->attributes.found("DIMENSION_LIST",at[0]);
		at[0].value.print(ss[0],istream.getReferenceTablePointer());
		ds[1]->attributes.found("DIMENSION_LIST",at[1]);
		at[1].value.print(ss[1],istream.getReferenceTablePointer());
		std::string sdum="["+re[0].name+", "+re[1].name+"]";
		if (ss[0].str() == sdum && ss[1].str() == sdum) {
		  parameter_matches=true;
		}
	    }
	  }
	}
	break;
    case 4:
	if (re[0].name == levid && re[1].name == latid && re[2].name == lonid) {
	  parameter_matches=true;
	}
	else {
	  auto can_continue=true;
	  if (re[0].name != levid && (ds[0]=istream.getDataset("/"+levid)) != nullptr) {
	    can_continue=false;
	    ds[0]->attributes.found("DIMENSION_LIST",at[0]);
	    at[0].value.print(ss[0],istream.getReferenceTablePointer());
	    if (ss[0].str() == "["+re[0].name+"]") {
		can_continue=true;
		ds[0]=nullptr;
		ss[0].str("");
	    }
	  }
	  if (can_continue && (ds[0]=istream.getDataset("/"+latid)) != nullptr && (ds[1]=istream.getDataset("/"+lonid)) != nullptr) {
	    ds[0]->attributes.found("DIMENSION_LIST",at[0]);
	    at[0].value.print(ss[0],istream.getReferenceTablePointer());
	    ds[1]->attributes.found("DIMENSION_LIST",at[1]);
	    at[1].value.print(ss[1],istream.getReferenceTablePointer());
	    std::string sdum="["+re[1].name+", "+re[2].name+"]";
	    if (ss[0].str() == sdum && ss[1].str() == sdum) {
		parameter_matches=true;
	    }
	  }
	}
	break;
  }
  return parameter_matches;
}

void addGriddedParametersToNetCDFLevelEntry(InputHDF5Stream& istream,std::string& gentry_key,std::string timeid,std::string latid,std::string lonid,std::string levid,ScanData& scan_data,const metautils::NcTime::TimeRangeEntry& tre,ParameterData& parameter_data)
{
  std::list<InputHDF5Stream::DatasetEntry> vars;
  InputHDF5Stream::Attribute attr;
  std::string time_method,tr_description,error;
  metautils::NcTime::TimeRange time_range;

// find all of the variables
  vars=istream.getDatasetsWithAttribute("DIMENSION_LIST");
  for (const auto& var : vars) {
    var.dataset->attributes.found("DIMENSION_LIST",attr);
    if (attr.value.class_ == 9 && attr.value.dim_sizes.size() == 1 && attr.value.dim_sizes[0] > 2 && (reinterpret_cast<unsigned char *>(attr.value.value))[0] == 7 && parameterMatchesDimensions(istream,attr,timeid,latid,lonid,levid)) {
	time_method=getGriddedTimeMethod(var.dataset,timeid);
	if (time_method.length() == 0 || (myequalf(time_bounds.t1,0,0.0001) && myequalf(time_bounds.t1,time_bounds.t2,0.0001))) {
	  time_range.first_valid_datetime=tre.instantaneous->first_valid_datetime;
	  time_range.last_valid_datetime=tre.instantaneous->last_valid_datetime;
	}
	else {
	  if (time_bounds.changed) {
	    metautils::logError("time bounds changed","nc2xml",user,args.argsString);
	  }
	  time_range.first_valid_datetime=tre.bounded->first_valid_datetime;
	  time_range.last_valid_datetime=tre.bounded->last_valid_datetime;
	}
	time_method=strutils::capitalize(time_method);
	tr_description=metautils::NcTime::getGriddedNetCDFTimeRangeDescription(tre,time_data,time_method,error);
	if (error.length() > 0) {
	  metautils::logError(error,"nc2xml",user,args.argsString);
	}
	tr_description=strutils::capitalize(tr_description);
	if (strutils::has_ending(gentry_key,tr_description)) {
	  if (attr.value.dim_sizes[0] == 3 || attr.value.dim_sizes[0] == 4) {
	    param_entry->key="ds"+args.dsnum+":"+var.key;
	    addGriddedNetCDFParameter(var,scan_data,time_range,parameter_data,*tre.num_steps);
	    InvEntry ie;
	    if (!inv_P_table.found(param_entry->key,ie)) {
		ie.key=param_entry->key;
		ie.num=inv_P_table.size();
		inv_P_table.insert(ie);
	    }
	  }
	}
    }
  }
}

void updateLevelEntry(InputHDF5Stream& istream,const metautils::NcTime::TimeRangeEntry tre,std::string timeid,std::string latid,std::string lonid,std::string levid,ScanData& scan_data,ParameterData& parameter_data,unsigned char& level_write)
{
  std::list<InputHDF5Stream::DatasetEntry> vars;
  InputHDF5Stream::Attribute attr;
  std::string time_method,tr_description,error;
  metautils::NcTime::TimeRange time_range;

  vars=istream.getDatasetsWithAttribute("DIMENSION_LIST");
  for (const auto& var : vars) {
    var.dataset->attributes.found("DIMENSION_LIST",attr);
    if (attr.value.class_ == 9 && attr.value.dim_sizes.size() == 1 && attr.value.dim_sizes[0] > 2 && (reinterpret_cast<unsigned char *>(attr.value.value))[0] == 7 && parameterMatchesDimensions(istream,attr,timeid,latid,lonid,levid)) {
	param_entry->key="ds"+args.dsnum+":"+var.key;
	time_method=getGriddedTimeMethod(var.dataset,timeid);
	time_method=strutils::capitalize(time_method);
	if (!lentry->parameter_code_table.found(param_entry->key,*param_entry)) {
	  if (time_method.length() == 0 || (myequalf(time_bounds.t1,0,0.0001) && myequalf(time_bounds.t1,time_bounds.t2,0.0001))) {
	    time_range.first_valid_datetime=tre.instantaneous->first_valid_datetime;
	    time_range.last_valid_datetime=tre.instantaneous->last_valid_datetime;
	    addGriddedNetCDFParameter(var,scan_data,time_range,parameter_data,*tre.num_steps);
	  }
	  else {
	    if (time_bounds.changed)
		metautils::logError("time bounds changed","nc2xml",user,args.argsString);
	    time_range.first_valid_datetime=tre.bounded->first_valid_datetime;
	    time_range.last_valid_datetime=tre.bounded->last_valid_datetime;
	    addGriddedNetCDFParameter(var,scan_data,time_range,parameter_data,*tre.num_steps);
	  }
	  gentry->level_table.replace(*lentry);
	}
	else {
	  tr_description=metautils::NcTime::getGriddedNetCDFTimeRangeDescription(tre,time_data,time_method,error);
	  if (error.length() > 0)
	    metautils::logError(error,"nc2xml",user,args.argsString);
	  tr_description=strutils::capitalize(tr_description);
	  if (strutils::has_ending(gentry->key,tr_description)) {
	    if (time_method.length() == 0 || (myequalf(time_bounds.t1,0,0.0001) && myequalf(time_bounds.t1,time_bounds.t2,0.0001))) {
		if (tre.instantaneous->first_valid_datetime < param_entry->startDateTime)
		  param_entry->startDateTime=tre.instantaneous->first_valid_datetime;
		if (tre.instantaneous->last_valid_datetime > param_entry->endDateTime)
		  param_entry->endDateTime=tre.instantaneous->last_valid_datetime;
	    }
	    else {
		if (tre.bounded->first_valid_datetime < param_entry->startDateTime)
		  param_entry->startDateTime=tre.bounded->first_valid_datetime;
		if (tre.bounded->last_valid_datetime > param_entry->endDateTime)
		  param_entry->endDateTime=tre.bounded->last_valid_datetime;
	    }
	    param_entry->numTimeSteps+=*tre.num_steps;
	    lentry->parameter_code_table.replace(*param_entry);
	    gentry->level_table.replace(*lentry);
	  }
	}
	level_write=1;
	InvEntry ie;
	if (!inv_P_table.found(param_entry->key,ie)) {
	  ie.key=param_entry->key;
	  ie.num=inv_P_table.size();
	  inv_P_table.insert(ie);
	}
    }
  }
}

void getTimeBounds(const HDF5::DataArray& data_array,InputHDF5Stream::Dataset *ds,metautils::NcTime::TimeRangeEntry& tre)
{
  time_bounds.t1=getValueFromDataArray(data_array,0,ds);
  time_bounds.diff=getValueFromDataArray(data_array,1,ds)-time_bounds.t1;
  for (size_t n=2; n < data_array.num_values; n+=2) {
    if (!myequalf((getValueFromDataArray(data_array,n+1,ds)-getValueFromDataArray(data_array,n,ds)),time_bounds.diff)) {
	time_bounds.changed=true;
    }
  }
  time_bounds.t2=getValueFromDataArray(data_array,data_array.num_values-1,ds);
  std::string error;
  tre.bounded->first_valid_datetime=metautils::NcTime::getActualDateTime(time_bounds.t1,time_data,error);
  if (error.length() > 0) {
    metautils::logError(error,"hdf2xml",user,args.argsString);
  }
  tre.bounded->last_valid_datetime=metautils::NcTime::getActualDateTime(time_bounds.t2,time_data,error);
  if (error.length() > 0) {
    metautils::logError(error,"hdf2xml",user,args.argsString);
  }
}

void updateInventory(int unum,int gnum,const HDF5::DataArray& time_array,InputHDF5Stream::Dataset *times)
{
  InvEntry ie;
  if (!inv_L_table.found(lentry->key,ie)) {
    ie.key=lentry->key;
    ie.num=inv_L_table.size();
    inv_L_table.insert(ie);
  }
  std::stringstream inv_line;
  for (size_t n=0; n < time_array.num_values; ++n) {
    for (const auto& key : lentry->parameter_code_table.keys()) {
	InvEntry pie;
	inv_P_table.found(key,pie);
	inv_line.str("");
	std::string error;
	inv_line << "0|0|" << metautils::NcTime::getActualDateTime(getValueFromDataArray(time_array,n,times),time_data,error).toString("%Y%m%d%H%MM") << "|" << unum << "|" << gnum << "|" << ie.num << "|" << pie.num << "|0";
	inv_lines.emplace_back(inv_line.str());
    }
  }
}

void scanHDF5NetCDF4File(InputHDF5Stream& istream,ScanData& scan_data)
{
  std::list<InputHDF5Stream::DatasetEntry> vars;
  InputHDF5Stream::Dataset *ds,*times=nullptr,*lats,*lons,*fcst_ref_time_ds,*bnds_ds;
  InputHDF5Stream::Attribute attr,attr2;
  std::string timeid,fcst_ref_timeid,timeboundsid,climoboundsid,levid,sdum,time_method,tr_description,error;
  std::deque<std::string> latid,lonid;
  metautils::ncLevel::LevelInfo level_info;
  std::deque<std::string> sp,sp2;
  long long dt;
  my::map<metautils::StringEntry> unique_level_ID_table;
  metautils::StringEntry se;
  my::map<metautils::NcTime::TimeRangeEntry> time_range_table;
  metautils::NcTime::TimeRangeEntry tre;
  HDF5::DataArray data_array,time_array,lat_array,lon_array,fcst_ref_time_array,bnds_array;
  int n,m,l,num_levels;
  Grid::GridDimensions dim;
  Grid::GridDefinition def;
  std::list<std::string> map_contents;
  my::map<metautils::StringEntry> gentry_table;
  ParameterData parameter_data;
  double ddum;
  InputHDF5Stream::ReferenceEntry re,re2,re3;
  bool found_time;

  found_time=false;
  gridInitialize();
  metadata::openInventory(inv,&inv_file,"hdf2xml",user);
  scan_data.map_name=getRemoteWebFile("http://rda.ucar.edu/metadata/ParameterTables/netCDF4.ds"+args.dsnum+".xml",tdir->name());
// rename the parameter map so that it is not overwritten by the level map,
//   which has the same name
  if (scan_data.map_name.length() > 0) {
    std::stringstream oss,ess;
    mysystem2("/bin/mv "+scan_data.map_name+" "+scan_data.map_name+".p",oss,ess);
    if (ess.str().length() > 0) {
	metautils::logError("unable to rename parameter map; error - '"+ess.str()+"'","nc2xml",user,args.argsString);
    }
    scan_data.map_name+=".p";
    if (parameter_data.map.fill(scan_data.map_name)) {
	scan_data.found_map=true;
    }
  }
  vars=istream.getDatasetsWithAttribute("CLASS=DIMENSION_SCALE");
  for (const auto& var : vars) {
    if (var.dataset->attributes.found("units",attr) && attr.value.class_ == 3) {
	sdum=reinterpret_cast<char *>(attr.value.value);
	if (strutils::contains(sdum,"since")) {
	  if (found_time) {
	    metautils::logError("time was already identified - don't know what to do with variable: "+var.key,"hdf2xml",user,args.argsString);
	  }
	  for (const auto& key : var.dataset->attributes.keys()) {
	    var.dataset->attributes.found(key,attr);
	    if (attr.value.class_ == 3) {
		if (key == "bounds") {
		  timeboundsid=reinterpret_cast<char *>(attr.value.value);
		  break;
		}
		else if (key == "climatology") {
		  climoboundsid=reinterpret_cast<char *>(attr.value.value);
		  break;
		}
	    }
	  }
	  time_data.units=sdum.substr(0,sdum.find("since"));
	  strutils::trim(time_data.units);
	  timeid=var.key;
	  sdum=sdum.substr(sdum.find("since")+5);
	  strutils::replace_all(sdum,"T"," ");
	  strutils::trim(sdum);
	  if (strutils::has_ending(sdum,"Z")) {
	    strutils::chop(sdum);
	  }
	  sp=strutils::split(sdum);
	  sp2=strutils::split(sp[0],"-");
	  if (sp2.size() != 3) {
	    metautils::logError("bad netcdf date in units for time","hdf2xml",user,args.argsString);
	  }
	  dt=std::stoi(sp2[0])*10000000000+std::stoi(sp2[1])*100000000+std::stoi(sp2[2])*1000000;
	  if (sp.size() > 1) {
	    sp2=strutils::split(sp[1],":");
	    dt+=std::stoi(sp2[0])*10000;
	    if (sp2.size() > 1) {
		dt+=std::stoi(sp2[1])*100;
	    }
	    if (sp2.size() > 2) {
		dt+=static_cast<long long>(std::stof(sp2[2]));
	    }
	  }
	  time_data.reference.set(dt);
	  found_time=true;
	}
	else if (sdum == "degrees_north") {
	  latid.push_back(var.key);
	}
	else if (sdum == "degrees_east") {
	  lonid.push_back(var.key);
	}
	else if (!unique_level_ID_table.found(var.key,se)) {
	  level_info.ID.push_back(var.key);
	  if (var.dataset->attributes.found("long_name",attr) && attr.value.class_ == 3) {
	    level_info.description.push_back(reinterpret_cast<char *>(attr.value.value));
	  }
	  level_info.units.push_back(sdum);
	  level_info.write.push_back(0);
	  se.key=var.key;
	  unique_level_ID_table.insert(se);
	}
    }
    else if (var.dataset->attributes.found("positive",attr) && attr.value.class_ == 3 && !unique_level_ID_table.found(var.key,se)) {
	level_info.ID.push_back(var.key);
	if (var.dataset->attributes.found("long_name",attr) && attr.value.class_ == 3) {
	  level_info.description.push_back(reinterpret_cast<char *>(attr.value.value));
	}
	level_info.units.push_back("");
	level_info.write.push_back(0);
	se.key=var.key;
	unique_level_ID_table.insert(se);
    }
  }
// check for forecasts
  vars=istream.getDatasetsWithAttribute("standard_name=forecast_reference_time");
  if (vars.size() > 1) {
    metautils::logError("multiple forecast reference times","hdf2xml",user,args.argsString);
  }
  else if (vars.size() > 0) {
    auto var=vars.front();
    if (var.dataset->attributes.found("units",attr) && attr.value.class_ == 3) {
	sdum=reinterpret_cast<char *>(attr.value.value);
	if (strutils::contains(sdum,"since")) {
	  fcst_ref_time_data.units=sdum.substr(0,sdum.find("since"));
	  strutils::trim(fcst_ref_time_data.units);
	  if (fcst_ref_time_data.units != time_data.units) {
	    metautils::logError("time and forecast reference time have different units","hdf2xml",user,args.argsString);
	  }
	  fcst_ref_timeid=var.key;
	  sdum=sdum.substr(sdum.find("since")+5);
	  strutils::replace_all(sdum,"T"," ");
	  strutils::trim(sdum);
	  if (strutils::has_ending(sdum,"Z")) {
	    strutils::chop(sdum);
	  }
	  sp=strutils::split(sdum);
	  sp2=strutils::split(sp[0],"-");
	  if (sp2.size() != 3) {
	    metautils::logError("bad netcdf date in units for forecast_reference_time","hdf2xml",user,args.argsString);
	  }
	  dt=std::stoi(sp2[0])*10000000000+std::stoi(sp2[1])*100000000+std::stoi(sp2[2])*1000000;
	  if (sp.size() > 1) {
	    sp2=strutils::split(sp[1],":");
	    dt+=std::stoi(sp2[0])*10000;
	    if (sp2.size() > 1) {
		dt+=std::stoi(sp2[1])*100;
	    }
	    if (sp2.size() > 2) {
		dt+=static_cast<long long>(std::stof(sp2[2]));
	    }
	  }
	  fcst_ref_time_data.reference.set(dt);
	}
    }
  }
  if (latid.size() == 0 && lonid.size() == 0) {
    vars=istream.getDatasetsWithAttribute("units=degrees_north");
    if (vars.size() == 0) {
	vars=istream.getDatasetsWithAttribute("units=degree_north");
    }
    for (const auto& v : vars) {
	latid.push_back(v.key);
    }
    vars=istream.getDatasetsWithAttribute("units=degrees_east");
    if (vars.size() == 0) {
	vars=istream.getDatasetsWithAttribute("units=degree_east");
    }
    for (const auto& v : vars) {
	lonid.push_back(v.key);
    }
  }
  if (levid.size() == 0) {
    vars=istream.getDatasetsWithAttribute("units=Pa");
    for (const auto& v : vars) {
	if (v.dataset->attributes.found("DIMENSION_LIST",attr) && attr.value.dim_sizes.size() == 1 && attr.value.dim_sizes[0] == 1 && attr.value.class_ == 9) {
	  level_info.ID.push_back(v.key);
	  level_info.description.push_back("Pressure Level");
	  level_info.units.push_back("Pa");
	  level_info.write.push_back(0);
	}
    }
    vars=istream.getDatasetsWithAttribute("positive");
    for (const auto& v : vars) {
	if (v.dataset->attributes.found("DIMENSION_LIST",attr) && attr.value.dim_sizes.size() == 1 && attr.value.dim_sizes[0] == 1 && attr.value.class_ == 9) {
	  level_info.ID.push_back(v.key);
	  if (v.dataset->attributes.found("description",attr) && attr.value.class_ == 3) {
	    level_info.description.push_back(reinterpret_cast<char *>(attr.value.value));
	  }
	  else {
	    level_info.description.push_back("");
	  }
	  if (v.dataset->attributes.found("units",attr) && attr.value.class_ == 3) {
	    level_info.units.push_back(reinterpret_cast<char *>(attr.value.value));
	  }
	  else {
	    level_info.units.push_back("");
	  }
	  level_info.write.push_back(0);
	}
    }
  }
  level_info.ID.push_back("sfc");
  level_info.description.push_back("Surface");
  level_info.units.push_back("");
  level_info.write.push_back(0);
  if (found_time && latid.size() > 0 && lonid.size() > 0) {
    if (time_range_table.size() == 0) {
	tre.key=-1;
	tre.unit=new int;
	*tre.unit=-1;
	tre.num_steps=new int;
	tre.instantaneous=new metautils::NcTime::TimeRange;
	if ( (times=istream.getDataset("/"+timeid)) == NULL) {
	  metautils::logError("unable to access the /"+timeid+" dataset for the data temporal range","hdf2xml",user,args.argsString);
	}
	time_array.fill(istream,*times);
	nctime.t1=getValueFromDataArray(time_array,0,times);
	nctime.t2=getValueFromDataArray(time_array,time_array.num_values-1,times);
	tre.instantaneous->first_valid_datetime=metautils::NcTime::getActualDateTime(nctime.t1,time_data,error);
	if (error.length() > 0) {
	  metautils::logError(error,"hdf2xml",user,args.argsString);
	}
	tre.instantaneous->last_valid_datetime=metautils::NcTime::getActualDateTime(nctime.t2,time_data,error);
	if (error.length() > 0) {
	  metautils::logError(error,"hdf2xml",user,args.argsString);
	}
	*tre.num_steps=time_array.num_values;
	if (fcst_ref_timeid.length() > 0) {
	  if ( (fcst_ref_time_ds=istream.getDataset("/"+fcst_ref_timeid)) == nullptr) {
	    metautils::logError("unable to access the /"+fcst_ref_timeid+" dataset for the forecast reference times","hdf2xml",user,args.argsString);
	  }
	  fcst_ref_time_array.fill(istream,*fcst_ref_time_ds);
	  if (fcst_ref_time_array.num_values != time_array.num_values) {
	    metautils::logError("number of forecast reference times does not equal number of times","hdf2xml",user,args.argsString);
	  }
	  for (size_t n=0; n < time_array.num_values; ++n) {
	    m=getValueFromDataArray(time_array,n,times)-getValueFromDataArray(fcst_ref_time_array,n,fcst_ref_time_ds);
	    if (m > 0) {
		if (static_cast<int>(tre.key) == -1) {
		  tre.key=-m*100;
		}
		if ( (-m*100) != static_cast<int>(tre.key)) {
		  metautils::logError("forecast period changed","hdf2xml",user,args.argsString);
		}
	    }
	    else if (m < 0) {
		metautils::logError("found a time value that is less than the forecast reference time value","hdf2xml",user,args.argsString);
	    }
	  }
	}
	if (timeboundsid.length() > 0) {
	  tre.bounded=new metautils::NcTime::TimeRange;
	  if ( (ds=istream.getDataset("/"+timeboundsid)) == NULL) {
	    metautils::logError("unable to access the /"+timeboundsid+" dataset for the time bounds","hdf2xml",user,args.argsString);
	  }
	  data_array.fill(istream,*ds);
	  if (data_array.num_values > 0) {
	    getTimeBounds(data_array,ds,tre);
	  }
	}
	else if (climoboundsid.length() > 0) {
	  tre.bounded=new metautils::NcTime::TimeRange;
	  if ( (ds=istream.getDataset("/"+climoboundsid)) == NULL) {
	    metautils::logError("unable to access the /"+climoboundsid+" dataset for the climatology bounds","hdf2xml",user,args.argsString);
	  }
	  data_array.fill(istream,*ds);
	  if (data_array.num_values > 0)
	    getTimeBounds(data_array,ds,tre);
	    tre.key=(tre.bounded->last_valid_datetime).getYearsSince(tre.bounded->first_valid_datetime);
	    tre.instantaneous->last_valid_datetime=(tre.bounded->last_valid_datetime).yearsSubtracted(tre.key);
	    if (tre.instantaneous->last_valid_datetime == tre.bounded->first_valid_datetime) {
		*tre.unit=3;
	    }
	    else if ((tre.instantaneous->last_valid_datetime).getMonthsSince(tre.bounded->first_valid_datetime) == 3) {
		*tre.unit=2;
	    }
	    else if ((tre.instantaneous->last_valid_datetime).getMonthsSince(tre.bounded->first_valid_datetime) == 1) {
		*tre.unit=1;
	    }
	    else {
		metautils::logError("unable to determine climatology unit","hdf2xml",user,args.argsString);
	    }
// COARDS convention for climatology over all-available years
	    if ((tre.instantaneous->first_valid_datetime).getYear() == 0) {
		tre.key=0x7fffffff;
	    }
	}
	if (time_data.units == "months") {
	  if ((tre.instantaneous->first_valid_datetime).getDay() == 1 && (tre.instantaneous->first_valid_datetime).getTime() == 0) {
	    tre.instantaneous->last_valid_datetime.addSeconds(getDaysInMonth((tre.instantaneous->last_valid_datetime).getYear(),(tre.instantaneous->last_valid_datetime).getMonth(),time_data.calendar)*86400-1,time_data.calendar);
	  }
	  if (timeboundsid.length() > 0) {
	    if ((tre.bounded->first_valid_datetime).getDay() == 1) {
		tre.bounded->last_valid_datetime.addDays(getDaysInMonth((tre.bounded->last_valid_datetime).getYear(),(tre.bounded->last_valid_datetime).getMonth(),time_data.calendar)-1,time_data.calendar);
	    }
	  }
	  else if (climoboundsid.length() > 0) {
	    if ((tre.bounded->first_valid_datetime).getDay() == (tre.bounded->last_valid_datetime).getDay() && (tre.bounded->first_valid_datetime).getTime() == 0 && (tre.bounded->last_valid_datetime).getTime() == 0) {
		tre.bounded->last_valid_datetime.subtractSeconds(1);
	    }
	  }
	}
	time_range_table.insert(tre);
    }
    if (latid.size() != lonid.size()) {
	metautils::logError("unequal number of latitude and longitude coordinate variables","hdf2xml",user,args.argsString);
    }
    for (n=0; n < static_cast<int>(latid.size()); ++n) {
	if ( (lats=istream.getDataset("/"+latid[n])) == NULL) {
	  metautils::logError("unable to access the /"+latid[n]+" dataset for the latitudes","hdf2xml",user,args.argsString);
	}
	if ( (lons=istream.getDataset("/"+lonid[n])) == NULL) {
	  metautils::logError("unable to access the /"+lonid[n]+" dataset for the latitudes","hdf2xml",user,args.argsString);
	}
	lat_array.fill(istream,*lats);
	lon_array.fill(istream,*lons);
	def.slatitude=getValueFromDataArray(lat_array,0,lats);
	def.slongitude=getValueFromDataArray(lon_array,0,lons);
	if (lats->attributes.found("DIMENSION_LIST",attr) && attr.value.dim_sizes.size() == 1 && attr.value.dim_sizes[0] == 2 && attr.value.class_ == 9 && lons->attributes.found("DIMENSION_LIST",attr2) && attr2.value.dim_sizes.size() == 1 && attr2.value.dim_sizes[0] == 2 && attr2.value.class_ == 9) {
	  if ( (reinterpret_cast<unsigned char *>(attr.value.value))[0] == 7 && (reinterpret_cast<unsigned char *>(attr2.value.value))[0] == 7) {
	    if (istream.getReferenceTablePointer()->found(HDF5::getValue(&(reinterpret_cast<unsigned char *>(attr.value.value))[1],attr.value.precision),re) && istream.getReferenceTablePointer()->found(HDF5::getValue(&(reinterpret_cast<unsigned char *>(attr2.value.value))[1],attr2.value.precision),re2) && re.name == re2.name && istream.getReferenceTablePointer()->found(HDF5::getValue(&(reinterpret_cast<unsigned char *>(attr.value.value))[1+attr.value.precision],attr.value.precision),re2) && istream.getReferenceTablePointer()->found(HDF5::getValue(&(reinterpret_cast<unsigned char *>(attr2.value.value))[1+attr2.value.precision],attr2.value.precision),re3) && re2.name == re3.name) {
		if ( (ds=istream.getDataset("/"+re.name)) == NULL || !ds->attributes.found("NAME",attr) || attr.value.class_ != 3) {
		  metautils::logError("(1)unable to determine grid definition from '"+latid[n]+"' and '"+lonid[n]+"'","hdf2xml",user,args.argsString);
		}
		sp=strutils::split(std::string(reinterpret_cast<char *>(attr.value.value)));
		if (sp.size() == 11) {
		  dim.y=std::stoi(sp[10]);
		}
		else {
		  metautils::logError("(2)unable to determine grid definition from '"+latid[n]+"' and '"+lonid[n]+"'","hdf2xml",user,args.argsString);
		}
		if ( (ds=istream.getDataset("/"+re2.name)) == NULL || !ds->attributes.found("NAME",attr) || attr.value.class_ != 3) {
		  metautils::logError("(3)unable to determine grid definition from '"+latid[n]+"' and '"+lonid[n]+"'","hdf2xml",user,args.argsString);
		}
		sp=strutils::split(std::string(reinterpret_cast<char *>(attr.value.value)));
		if (sp.size() == 11) {
		  dim.x=std::stoi(sp[10]);
		}
		else {
		  metautils::logError("(4)unable to determine grid definition from '"+latid[n]+"' and '"+lonid[n]+"'","hdf2xml",user,args.argsString);
		}
	    }
	    else {
		metautils::logError("(5)unable to determine grid definition from '"+latid[n]+"' and '"+lonid[n]+"'","hdf2xml",user,args.argsString);
	    }
	    auto center_x=dim.x/2;
	    auto center_y=dim.y/2;
	    auto xm=center_x-1;
	    auto ym=center_y-1;
	    if (myequalf(getValueFromDataArray(lat_array,ym*dim.x+xm,lats),getValueFromDataArray(lat_array,center_y*dim.x+xm,lats),0.00001) && myequalf(getValueFromDataArray(lat_array,center_y*dim.x+xm,lats),getValueFromDataArray(lat_array,center_y*dim.x+center_x,lats),0.00001) && myequalf(getValueFromDataArray(lat_array,center_y*dim.x+center_x,lats),getValueFromDataArray(lat_array,ym*dim.x+center_x,lats),0.00001) && myequalf(fabs(getValueFromDataArray(lon_array,ym*dim.x+xm,lons))+fabs(getValueFromDataArray(lon_array,center_y*dim.x+xm,lons))+fabs(getValueFromDataArray(lon_array,center_y*dim.x+center_x,lons))+fabs(getValueFromDataArray(lon_array,ym*dim.x+center_x,lons)),360.,0.00001)) {
		def.type=Grid::polarStereographicType;
		if (getValueFromDataArray(lat_array,ym*dim.x+xm,lats) >= 0.) {
		  def.projection_flag=0;
		  def.llatitude=60.;
		}
		else {
		  def.projection_flag=1;
		  def.llatitude=-60.;
		}
		def.olongitude=lroundf(getValueFromDataArray(lon_array,ym*dim.x+xm,lons)+45.);
		if (def.olongitude > 180.) {
		  def.olongitude-=360.;
		}
// look for dx and dy at the 60-degree parallel
 		double min_fabs=999.,f;
		int min_m=0;
		for (size_t m=0; m < lat_array.num_values; ++m) {
		  if ( (f=fabs(def.llatitude-getValueFromDataArray(lat_array,m,lats))) < min_fabs) {
			min_fabs=f;
			min_m=m;
		  }
		}
		double rad=3.141592654/180.;
// great circle formula:
// //  theta=2*arcsin[ sqrt( sin^2(delta_phi/2) + cos(phi_1)*cos(phi_2)*sin^2(delta_lambda/2) ) ]
// //  phi_1 and phi_2 are latitudes
// //  lambda_1 and lambda_2 are longitudes
// //  dist = 6372.8 * theta
// //  6372.8 is radius of Earth in km
		def.dx=lroundf(asin(sqrt(sin(fabs(getValueFromDataArray(lat_array,min_m,lats)-getValueFromDataArray(lat_array,min_m+1,lats))/2.*rad)*sin(fabs(getValueFromDataArray(lat_array,min_m,lats)-getValueFromDataArray(lat_array,min_m+1,lats))/2.*rad)+sin(fabs(getValueFromDataArray(lon_array,min_m,lons)-getValueFromDataArray(lon_array,min_m+1,lons))/2.*rad)*sin(fabs(getValueFromDataArray(lon_array,min_m,lons)-getValueFromDataArray(lon_array,min_m+1,lons))/2.*rad)*cos(getValueFromDataArray(lat_array,min_m,lats)*rad)*cos(getValueFromDataArray(lat_array,min_m+1,lats)*rad)))*12745.6);
		def.dy=lroundf(asin(sqrt(sin(fabs(getValueFromDataArray(lat_array,min_m,lats)-getValueFromDataArray(lat_array,min_m+dim.x,lats))/2.*rad)*sin(fabs(getValueFromDataArray(lat_array,min_m,lats)-getValueFromDataArray(lat_array,min_m+dim.x,lats))/2.*rad)+sin(fabs(getValueFromDataArray(lon_array,min_m,lons)-getValueFromDataArray(lon_array,min_m+dim.x,lons))/2.*rad)*sin(fabs(getValueFromDataArray(lon_array,min_m,lons)-getValueFromDataArray(lon_array,min_m+dim.x,lons))/2.*rad)*cos(getValueFromDataArray(lat_array,min_m,lats)*rad)*cos(getValueFromDataArray(lat_array,min_m+dim.x,lats)*rad)))*12745.6);
	    }
	    else if ((dim.x % 2) == 1 && myequalf(getValueFromDataArray(lon_array,ym*lon_array.dimensions[1]+center_x,lons),getValueFromDataArray(lon_array,(center_y+1)*lon_array.dimensions[1]+center_x,lons),0.00001) && myequalf(getValueFromDataArray(lat_array,center_y*lon_array.dimensions[1]+xm,lats),getValueFromDataArray(lat_array,center_y*lon_array.dimensions[1]+center_x+1,lats),0.00001)) {
		def.type=Grid::lambertConformalType;
		def.llatitude=def.stdparallel1=def.stdparallel2=lround(getValueFromDataArray(lat_array,center_y*lon_array.dimensions[1]+center_x,lats));
		if (def.llatitude >= 0.) {
		  def.projection_flag=0;
		}
		else {
		  def.projection_flag=1;
		}
		def.olongitude=lround(getValueFromDataArray(lon_array,center_y*lon_array.dimensions[1]+center_x,lons));
		def.dx=def.dy=lround(111.1*cos(getValueFromDataArray(lat_array,center_y*lon_array.dimensions[1]+center_x,lats)*3.141592654/180.)*(getValueFromDataArray(lon_array,center_y*lon_array.dimensions[1]+center_x+1,lons)-getValueFromDataArray(lon_array,center_y*lon_array.dimensions[1]+center_x,lons)));
	    }
	    else if ((dim.x % 2) == 0 && myequalf((getValueFromDataArray(lon_array,ym*lon_array.dimensions[1]+xm,lons)+getValueFromDataArray(lon_array,ym*lon_array.dimensions[1]+center_x,lons)),(getValueFromDataArray(lon_array,(center_y+1)*lon_array.dimensions[1]+xm,lons)+getValueFromDataArray(lon_array,(center_y+1)*lon_array.dimensions[1]+center_x,lons)),0.00001) && myequalf(getValueFromDataArray(lat_array,center_y*lon_array.dimensions[1]+xm,lats),getValueFromDataArray(lat_array,center_y*lon_array.dimensions[1]+center_x,lats))) {
		def.type=Grid::lambertConformalType;
		def.llatitude=def.stdparallel1=def.stdparallel2=lround(getValueFromDataArray(lat_array,center_y*lon_array.dimensions[1]+center_x,lats));
		if (def.llatitude >= 0.) {
		  def.projection_flag=0;
		}
		else {
		  def.projection_flag=1;
		}
		def.olongitude=lround((getValueFromDataArray(lon_array,center_y*lon_array.dimensions[1]+xm,lons)+getValueFromDataArray(lon_array,center_y*lon_array.dimensions[1]+center_x,lons))/2.);
		def.dx=def.dy=lround(111.1*cos(getValueFromDataArray(lat_array,center_y*lon_array.dimensions[1]+center_x-1,lats)*3.141592654/180.)*(getValueFromDataArray(lon_array,center_y*lon_array.dimensions[1]+center_x,lons)-getValueFromDataArray(lon_array,center_y*lon_array.dimensions[1]+center_x-1,lons)));
	    }
	    else {
std::cerr.precision(10);
std::cerr << getValueFromDataArray(lon_array,ym*lon_array.dimensions[1]+center_x,lons) << std::endl;
std::cerr << getValueFromDataArray(lon_array,(center_y+1)*lon_array.dimensions[1]+center_x,lons) << std::endl;
std::cerr << myequalf(getValueFromDataArray(lon_array,ym*lon_array.dimensions[1]+center_x,lons),getValueFromDataArray(lon_array,(center_y+1)*lon_array.dimensions[1]+center_x,lons),0.00001) << std::endl;
std::cerr << getValueFromDataArray(lat_array,center_y*lon_array.dimensions[1]+xm,lats) << std::endl;
std::cerr << getValueFromDataArray(lat_array,center_y*lon_array.dimensions[1]+center_x+1,lats) << std::endl;
std::cerr << myequalf(getValueFromDataArray(lat_array,center_y*lon_array.dimensions[1]+xm,lats),getValueFromDataArray(lat_array,center_y*lon_array.dimensions[1]+center_x+1,lats),0.00001) << std::endl;
		metautils::logError("(6)unable to determine grid definition from '"+latid[n]+"' and '"+lonid[n]+"'","hdf2xml",user,args.argsString);
	    }
	  }
	  else {
	    metautils::logError("(7)unable to determine grid definition from '"+latid[n]+"' and '"+lonid[n]+"'","hdf2xml",user,args.argsString);
	  }
	}
	else {
	  def.type=Grid::latitudeLongitudeType;
	  dim.y=lat_array.num_values;
	  dim.x=lon_array.num_values;
	  def.elatitude=getValueFromDataArray(lat_array,dim.y-1,lats);
	  def.laincrement=fabs((def.elatitude-def.slatitude)/(dim.y-1));
	  def.elongitude=getValueFromDataArray(lon_array,dim.x-1,lons);
	  def.loincrement=fabs((def.elongitude-def.slongitude)/(dim.x-1));
	}
	for (m=0; m < static_cast<int>(level_info.ID.size()); ++m) {
	  gentry_table.clear();
	  levid=level_info.ID[m];
	  bnds_ds=nullptr;
	  if (m == static_cast<int>(level_info.ID.size()-1) && levid == "sfc") {
	    num_levels=1;
	    ds=nullptr;
	  }
	  else {
	    if ( (ds=istream.getDataset("/"+levid)) == nullptr) {
		metautils::logError("unable to access the /"+levid+" dataset for level information","hdf2xml",user,args.argsString);
	    }
	    data_array.fill(istream,*ds);
	    num_levels=data_array.num_values;
	    if (ds->attributes.found("bounds",attr) && attr.value.class_ == 3) {
		sdum=reinterpret_cast<char *>(attr.value.value);
		if ( (bnds_ds=istream.getDataset("/"+sdum)) == nullptr) {
		  metautils::logError("unable to get bounds for level '"+levid+"'","hdf2xml",user,args.argsString);
		}
		bnds_array.fill(istream,*bnds_ds);
	    }
	  }
	  for (const auto& key : time_range_table.keys()) {
	    time_range_table.found(key,tre);
	    addGriddedLatLonKeys(gentry_table,dim,def,tre,timeid,istream);
	    for (const auto& key2 : gentry_table.keys()) {
		gentry->key=key2;
		sp=strutils::split(gentry->key,"<!>");
		InvEntry uie,gie;
		if (inv.is_open()) {
		  inv_U_table.found(sp.back(),uie);
		  gie.key=sp[0];
		  for (size_t nn=1; nn < sp.size()-1; ++nn) {
		    gie.key+=","+sp[nn];
		  }
		  inv_G_table.found(gie.key,gie);
		}
		if (!grid_table->found(gentry->key,*gentry)) {
// new grid
		  gentry->level_table.clear();
		  lentry->parameter_code_table.clear();
		  param_entry->numTimeSteps=0;
		  addGriddedParametersToNetCDFLevelEntry(istream,gentry->key,timeid,latid[n],lonid[n],levid,scan_data,tre,parameter_data);
		  if (lentry->parameter_code_table.size() > 0) {
		    for (l=0; l < num_levels; ++l) {
			lentry->key="ds"+args.dsnum+","+levid+":";
			if (bnds_ds == nullptr) {
			  if (ds == nullptr) {
			    ddum=0;
			  }
			  else {
			    ddum=getValueFromDataArray(data_array,l,ds);
			  }
			  if (myequalf(ddum,static_cast<int>(ddum),0.001)) {
			    lentry->key+=strutils::itos(ddum);
			  }
			  else {
			    lentry->key+=strutils::ftos(ddum,3);
			  }
			}
			else {
			  ddum=getValueFromDataArray(bnds_array,l*2,bnds_ds);
			  if (myequalf(ddum,static_cast<int>(ddum),0.001)) {
			    lentry->key+=strutils::itos(ddum);
			  }
			  else {
			    lentry->key+=strutils::ftos(ddum,3);
			  }
			  ddum=getValueFromDataArray(bnds_array,l*2+1,bnds_ds);
			  lentry->key+=":";
			  if (myequalf(ddum,static_cast<int>(ddum),0.001)) {
			    lentry->key+=strutils::itos(ddum);
			  }
			  else {
			    lentry->key+=strutils::ftos(ddum,3);
			  }
			}
			gentry->level_table.insert(*lentry);
			level_info.write[m]=1;
			if (inv.is_open()) {
			  updateInventory(uie.num,gie.num,time_array,times);
			}
		    }
		  }
		  if (gentry->level_table.size() > 0) {
		    grid_table->insert(*gentry);
		  }
 		}
		else {
// existing grid - needs update
		  for (l=0; l < num_levels; ++l) {
		    lentry->key="ds"+args.dsnum+","+levid+":";
		    if (ds == nullptr) {
			ddum=0;
		    }
		    else {
			ddum=getValueFromDataArray(data_array,l,ds);
		    }
		    if (myequalf(ddum,static_cast<int>(ddum),0.001)) {
			lentry->key+=strutils::itos(ddum);
		    }
		    else {
			lentry->key+=strutils::ftos(ddum,3);
		    }
		    if (!gentry->level_table.found(lentry->key,*lentry)) {
			lentry->parameter_code_table.clear();
			addGriddedParametersToNetCDFLevelEntry(istream,gentry->key,timeid,latid[n],lonid[n],levid,scan_data,tre,parameter_data);
			if (lentry->parameter_code_table.size() > 0) {
			  gentry->level_table.insert(*lentry);
			  level_info.write[m]=1;
			}
		    }
		    else {
 			updateLevelEntry(istream,tre,timeid,latid[n],lonid[n],levid,scan_data,parameter_data,level_info.write[m]);
		    }
		    if (level_info.write[m] == 1 && inv.is_open()) {
			updateInventory(uie.num,gie.num,time_array,times);
		    }
		  }
		  grid_table->replace(*gentry);
		}
	    }
	  }
	}
	error=metautils::ncLevel::writeLevelMap(args.dsnum,level_info);
	if (error.length() > 0) {
	  metautils::logError(error,"hdf2xml",user,args.argsString);
	}
    }
  }
  write_type=GrML_type;
  if (grid_table->size() == 0) {
    if (found_time) {
	metautils::logError("No grids found - no content metadata will be generated","hdf2xml",user,args.argsString);
    }
    else {
	metautils::logError("Time coordinate variable not found - no content metadata will be generated","hdf2xml",user,args.argsString);
    }
  }
if (inv.is_open()) {
InvEntry ie;
ie.key="x";
ie.num=0;
inv_R_table.insert(ie);
}
}

void scanHDF5File(std::list<std::string>& filelist,ScanData& scan_data)
{
  InputHDF5Stream istream;
  std::string map_type,warning,error;

  for (const auto& file : filelist) {
    if (!istream.open(file.c_str())) {
	myerror+=" - file: '"+file+"'";
	exit(1);
    }
    if (args.format == "ispdhdf5") {
	scanISPDHDF5File(istream);
    }
    else if (args.format == "hdf5nc4") {
	scanHDF5NetCDF4File(istream,scan_data);
    }
    else if (args.format == "usarrthdf5") {
	scanUSArrayTransportableHDF5File(istream,scan_data);
    }
    else {
	std::cerr << "Error: bad data format specified" << std::endl;
	exit(1);
    }
    istream.close();
  }
  if (write_type == GrML_type) {
    cmd_type="GrML";
    if (!args.inventoryOnly) {
	metadata::GrML::writeGrML(*grid_table,"hdf2xml",user);
    }
    gridFinalize();
  }
  else if (write_type == ObML_type) {
    if (num_not_missing > 0) {
	args.format="hdf5";
	cmd_type="ObML";
	metadata::ObML::writeObML(ID_table,platform_table,"hdf2xml",user);
    }
    else {
	metautils::logError("all stations have missing location information - no usable data found; no content metadata will be saved for this file","hdf2xml",user,args.argsString);
    }
    obsFinalize();
  }
  if (write_type == GrML_type) {
    map_type="parameter";
  }
  else if (write_type == ObML_type) {
    map_type="dataType";
  }
  else {
    metautils::logError("scanHDF5File returned error: unknown map type","hdf2xml",user,args.argsString);
  }
  error=metautils::ncParameter::writeParameterMap(args.dsnum,scan_data.varlist,scan_data.var_changes_table,map_type,scan_data.map_name,scan_data.found_map,warning);
}

void scanFile()
{
  std::string file_format,error;
  std::list<std::string> filelist;
  ScanData scan_data;

  tfile=new TempFile;
  if (!tfile->open(directives.tempPath)) {
    metautils::logError("scanFile was not able to create a temporary file in "+directives.tempPath,"hdf2xml",user,args.argsString);
  }
  tdir=new TempDir;
  if (!tdir->create(directives.tempPath)) {
    metautils::logError("scanFile was not able to create a temporary directory in "+directives.tempPath,"hdf2xml",user,args.argsString);
  }
  if (!primaryMetadata::prepareFileForMetadataScanning(*tfile,*tdir,&filelist,file_format,error)) {
    metautils::logError("prepareFileForMetadataScanning() returned '"+error+"'","hdf2xml",user,args.argsString);
  }
  if (filelist.size() == 0) {
    filelist.push_back(tfile->name());
  }
  if (strutils::contains(args.format,"hdf4")) {
    scanHDF4File(filelist,scan_data);
  }
  else if (strutils::contains(args.format,"hdf5")) {
    scanHDF5File(filelist,scan_data);
  }
  else {
    std::cerr << "Error: bad data format specified" << std::endl;
    exit(1);
  }
}

int main(int argc,char **argv)
{
  std::string flags;

  if (argc < 6) {
    std::cerr << "usage: hdf2xml -f format -d [ds]nnn.n [options...] <path>" << std::endl;
    std::cerr << "\nrequired (choose one):" << std::endl;
    std::cerr << "HDF4 formats:" << std::endl;
    std::cerr << "-f quikscathdf4   NASA QuikSCAT HDF4" << std::endl;
    std::cerr << "HDF5 formats:" << std::endl;
    std::cerr << "-f ispdhdf5       NOAA International Surface Pressure Databank HDF5" << std::endl;
    std::cerr << "-f hdf5nc4        NetCDF4 with HDF5 storage" << std::endl;
    std::cerr << "-f usarrthdf5     EarthScope USArray Transportable Array Pressure Observations" << std::endl;
    std::cerr << "\nrequired:" << std::endl;
    std::cerr << "-d <nnn.n>       nnn.n is the dataset number to which the data file belongs" << std::endl;
    std::cerr << "\noptions" << std::endl;
    std::cerr << "-l <name>        name of the HPSS file on local disk (this avoids an HPSS read)" << std::endl;
    std::cerr << "-m <name>        name of member; <path> MUST be the name of a parent file that" << std::endl;
    std::cerr << "                   has support for direct member access" << std::endl;
    std::cerr << "required:" << std::endl;
    std::cerr << "<path>           full HPSS path or URL of the file to read" << std::endl;
    std::cerr << "                 - HPSS paths must begin with \"/FS/DSS\"" << std::endl;
    std::cerr << "                 - URLs must begin with \"http://{rda|dss}.ucar.edu\"" << std::endl;
    exit(1);
  }
  signal(SIGSEGV,segv_handler);
  signal(SIGINT,int_handler);
  args.argsString=getUnixArgsString(argc,argv,'%');
  metautils::readConfig("hdf2xml",user,args.argsString);
  parseArgs();
  atexit(cleanUp);
  metautils::cmd_register("hdf2xml",user);
  if (!args.overwriteOnly) {
    metautils::checkForExistingCMD("GrML");
    metautils::checkForExistingCMD("ObML");
  }
  scanFile();
  flags="-f";
  if (!args.inventoryOnly && strutils::has_beginning(args.path,"http://rda.ucar.edu")) {
    flags="-wf";
  }
  if (args.updateDB) {
    if (!args.updateSummary) {
	flags="-S "+flags;
    }
    if (!args.regenerate) {
	flags="-R "+flags;
    }
    if (cmd_type.length() == 0) {
	metautils::logError("content metadata type was not specified","hdf2xml",user,args.argsString);
    }
    std::stringstream oss,ess;
    if (mysystem2(directives.localRoot+"/bin/scm -d "+args.dsnum+" "+flags+" "+args.filename+"."+cmd_type,oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
  }
  if (inv.is_open()) {
    InvEntry ie;
    for (const auto& key : inv_U_table.keys()) {
	inv_U_table.found(key,ie);
	inv << "U<!>" << ie.num << "<!>" << key << std::endl;
    }
    for (const auto& key : inv_G_table.keys()) {
	inv_G_table.found(key,ie);
	inv << "G<!>" << ie.num << "<!>" << key << std::endl;
    }
    for (const auto& key : inv_L_table.keys()) {
	inv_L_table.found(key,ie);
	inv << "L<!>" << ie.num << "<!>" << key << std::endl;
    }
    for (const auto& key : inv_P_table.keys()) {
	inv_P_table.found(key,ie);
	inv << "P<!>" << ie.num << "<!>" << key << std::endl;
    }
    for (const auto& key : inv_R_table.keys()) {
	inv_R_table.found(key,ie);
	inv << "R<!>" << ie.num << "<!>" << key << std::endl;
    }
    inv << "-----" << std::endl;
    for (const auto& line : inv_lines) {
	inv << line << std::endl;
    }
    metadata::closeInventory(inv,inv_file,"GrML",true,true,"hdf2xml",user);
  }
}
