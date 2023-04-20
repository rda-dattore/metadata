#include <iostream>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <regex>
#include <utils.hpp>
#include <strutils.hpp>
#include <xml.hpp>
#include <MySQL.hpp>
#include <datetime.hpp>

/*
  cflag in search.GCMD_<concept_scheme>:
    0 = unchanged
    1 = added
    2 = changed
    9 = ignored by RDA
*/

const std::string USERNAME="dattore";
const std::string PASSWORD="8F5uXZ6b";
//const std::string KMS_API_URL="https://gcmdservices.gsfc.nasa.gov/kms";
const std::string KMS_API_URL="https://gcmd.earthdata.nasa.gov/kms";

struct Args {
  Args() : server("rda-db.ucar.edu","metadata","metadata",""),concept_scheme(),alias(),delete_flag(strutils::strand(3)),concept_schemes(),rda_keywords(),force_update(false),no_gcmd_update(false)
  {
    if (!server) {
      std::cerr << "Error: unable to connect to metadata database" << std::endl;
      exit(1);
    }
/*
  concept_schemes tuple:
    path start field in csv file
    path end field in csv file
    minimum number of fields required for a valid path
    true for only isLeaf=true nodes in /concept/concept_schemes/<concept_scheme>
      xml file, otherwise false
*/
    concept_schemes.emplace("instruments",std::make_tuple(4,5,1,true));
    concept_schemes.emplace("locations",std::make_tuple(0,4,2,false));
    concept_schemes.emplace("platforms",std::make_tuple(3,4,1,true));
    concept_schemes.emplace("projects",std::make_tuple(1,2,1,true));
    concept_schemes.emplace("providers",std::make_tuple(4,5,1,true));
    concept_schemes.emplace("sciencekeywords",std::make_tuple(0,5,4,false));
  }

  MySQL::Server server;
  std::string concept_scheme,alias,delete_flag;
  std::unordered_map<std::string,std::tuple<size_t,size_t,size_t,bool>> concept_schemes;
/*
  rda_keywords:
    short indicates: add keyword (1) or delete keyword (0)
*/
  std::unordered_map<std::string,short> rda_keywords;
  bool force_update,no_gcmd_update;
} args;

std::string sql_ready(const std::string& s)
{
  return strutils::substitute(s,"'","\\'");
}

void parse_args(int argc,char **argv)
{
  if (argc < 2) {
    std::cerr << "usage: update_gcmd [options] <concept_scheme>" << std::endl;
    std::cerr << "\noptions:" << std::endl;
    std::cerr << "--force-update       force an update even if last-update dates do not require it" << std::endl;
    std::cerr << "--add-rda <keyword>  add an RDA-specific keyword (repeatable)" << std::endl;
    std::cerr << "--no-gcmd            don't update from GCMD" << std::endl;
    std::cerr << std::endl;
    std::cerr << "-A <alias>           get the concepts for <concept_scheme> from <alias> instead;" << std::endl;
    std::cerr << "                     this helps when GCMD's database is corrupted and the API is" << std::endl;
    std::cerr << "                     serving the wrong keywords for a concept scheme" << std::endl;
    std::cerr << "\nvalid concept_schemes:" << std::endl;
    std::cerr << "  instruments" << std::endl;
    std::cerr << "  locations" << std::endl;
    std::cerr << "  platforms" << std::endl;
    std::cerr << "  projects" << std::endl;
    std::cerr << "  providers" << std::endl;
    std::cerr << "  sciencekeywords" << std::endl;
    exit(1);
  }
  auto arglist=strutils::split(unixutils::unix_args_string(argc,argv),":");
  auto next=arglist.begin();
  auto end=arglist.end();
  --end;
  for (; next != end; ++next) {
    if (next == arglist.end()) {
      std::cerr << "Error: missing concept_scheme" << std::endl;
      exit(1);
    }
    if (*next == "--force-update") {
      args.force_update=true;
    }
    else if (*next == "--add-rda") {
      ++next;
      args.rda_keywords.emplace(*next,1);
    }
    else if (*next == "--no-gcmd") {
      args.no_gcmd_update=true;
    }
    else if (*next == "-A") {
      ++next;
      args.alias=*next;
    }
  }
  args.concept_scheme=arglist.back();
  if (args.concept_schemes.find(args.concept_scheme) == args.concept_schemes.end() && args.concept_scheme != "all") {
    std::cerr << "Error: '" << args.concept_scheme << "' is not a valid concept_scheme" << std::endl;
    exit(1);
  }
}

bool passed_date_check() {
  std::stringstream oss,ess;
  unixutils::mysystem2("/bin/sh -c 'curl -k -s -S -o - \""+KMS_API_URL+"/concept_schemes\"'",oss,ess);
  if (!ess.str().empty()) {
    std::cerr << "Error getting GCMD updateDate (1):" << std::endl;
    std::cerr << ess.str() << std::endl;
    exit(1);
  }
  auto index=oss.str().find("<schemes");
  if (index == std::string::npos) {
    std::cerr << "Error getting GCMD updateDate (2):" << std::endl;
    std::cerr << oss.str() << std::endl;
    exit(1);
  }
  XMLSnippet update(oss.str().substr(oss.str().find("<schemes")));
  if (update) {
    auto gcmd_update_date=update.element("schemes/scheme@name="+args.concept_scheme).attribute_value("updateDate");
    MySQL::LocalQuery q("select update_time from information_schema.tables where table_schema = 'search' and table_name = 'GCMD_" + args.concept_scheme + "'");
    if (q.submit(args.server) == 0 && q.num_rows() == 1) {
      MySQL::Row row;
      q.fetch_row(row);
      auto update_time = row[0];
      if (update_time.empty()) {
        update_time = "0001-01-01";
      }
      auto uparts = strutils::split(update_time);
      std::cout << "GCMD '" << args.concept_scheme << "' update date: " << gcmd_update_date << std::endl;
      std::cout << "RDA database '" << args.concept_scheme << "' update date: " << uparts[0] << std::endl;
      if (gcmd_update_date < uparts[0]) {
        std::cout << "No update needed for '" << args.concept_scheme << "' at this time" << std::endl;
        if (!args.force_update) {
          return false;
        }
        else {
          std::cout << "  however, proceeding with update since --force-update was specified" << std::endl;
        }
      }
    } else {
      std::cerr << "Error getting GCMD_"+args.concept_scheme+" update date: " << q.error() << std::endl;
      exit(1);
    }
  }
  else {
    std::cerr << "Error parsing updates: " << update.parse_error() << std::endl;
    exit(1);
  }
  return true;
}

void update_keywords()
{
  std::cout << "Beginning update of '" << args.concept_scheme << "' at " << dateutils::current_date_time().to_string() << "..." << std::endl;
  std::stringstream oss,ess;
  unixutils::mysystem2("/bin/sh -c 'curl -k -s -S -o - \""+KMS_API_URL+"/concepts/concept_scheme/"+args.concept_scheme+"/?format=csv\"'",oss,ess);
  if (!ess.str().empty()) {
    std::cerr << "Error getting " << args.concept_scheme << ".csv" << std::endl;
    exit(1);
  }
  std::string version,revision_date;
  std::unordered_map<std::string,std::tuple<std::string,std::string,std::string>> csv_map;
  std::unordered_set<std::string> csv_ignore_set;
  auto csv_lines=strutils::split(oss.str(),"\n");
  if (!regex_search(csv_lines.front(),std::regex("^\"Keyword Version:"))) {
    std::cerr << "Error: invalid csv file format" << std::endl;
    exit(1);
  }
  else {
    auto parts=strutils::split(csv_lines.front(),"\",\"");
    strutils::replace_all(parts[0],"\"Keyword Version:","");
    strutils::trim(parts[0]);
    version=parts[0];
    if (!regex_search(parts[1],std::regex("^Revision:"))) {
      std::cerr << "Error: missing or invalid revision date" << std::endl;
      exit(1);
    }
    strutils::replace_all(parts[1],"Revision:","");
    strutils::trim(parts[1]);
    auto rparts=strutils::split(parts[1]);
    revision_date=rparts.front();
    csv_lines.pop_front();
  }
  std::vector<std::tuple<std::string,size_t,std::regex,std::string>> search_re_list;
  if (args.concept_scheme == "platforms") {
    MySQL::LocalQuery query("search_regexp,ObML_platformType","search.GCMD_platforms_to_RDA");
    if (query.submit(args.server) == 0) {
      MySQL::Row row;
      while (query.fetch_row(row)) {
        search_re_list.emplace_back(std::make_tuple(row[0],strutils::occurs(row[0],",")+1,std::regex(row[0]),row[1]));
      }
      std::sort(search_re_list.begin(),search_re_list.end(),
      [](std::tuple<std::string,size_t,std::regex,std::string>& left,std::tuple<std::string,size_t,std::regex,std::string>& right) -> bool
      {
        auto nleft=std::get<1>(left);
        auto nright=std::get<1>(right);
        if (nleft > nright) {
          return true;
        }
        else if (nleft < nright) {
          return false;
        }
        else {
          std::regex double_comma(",,");
          if (std::regex_search(std::get<0>(left),double_comma)) {
            if (std::regex_search(std::get<0>(right),double_comma)) {
              return (std::get<0>(left) <= std::get<0>(right));
            }
            else {
              return false;
            }
          }
          else {
            if (std::regex_search(std::get<0>(right),double_comma)) {
              return true;
            }
            else {
              return (std::get<0>(left) <= std::get<0>(right));
            }
          }
        }
      });
    }
    else {
      std::cerr << "Error getting search_re data" << std::endl;
      exit(1);
    }
  }
/*
else if (args.concept_scheme == "locations") {
strutils::replace_all(csv_lines.front(), ",Location_Subregion4", "");
}
*/
  auto fields=strutils::split(csv_lines.front(),",");
  auto check_len=fields.size();
  csv_lines.pop_front();
  auto path_data=args.concept_schemes[args.concept_scheme];
  for (auto& line : csv_lines) {
    if (!line.empty()) {
// remove the trailing '"'
      line.pop_back();
// ignore the leading '"'
      auto lparts=strutils::split(line.substr(1),"\",\"");
      if (lparts.size() != check_len) {
        std::cerr << "SKIPPING BAD CSV LINE: " << line << std::endl;
      }
      else {
// lparts.back() is the UUID
        if (csv_map.find(lparts.back()) == csv_map.end()) {
          if (lparts[std::get<0>(path_data)].size() > 0) {
            size_t last=std::get<0>(path_data);
            std::string path;
            if (lparts[std::get<0>(path_data)][0] == '"') {
              path=lparts[std::get<0>(path_data)].substr(1);
            }
            else {
              path=lparts[std::get<0>(path_data)];
            }
            size_t num_in_path=1;
            for (size_t n=std::get<0>(path_data)+1; n <= std::get<1>(path_data); ++n) {
              if (!lparts[n].empty()) {
                path+=" > "+lparts[n];
                last=n;
                ++num_in_path;
              }
            }
            if (num_in_path >= std::get<2>(path_data)) {
              if (args.concept_scheme == "sciencekeywords" && !std::regex_search(path,std::regex("^EARTH SCIENCE > "))) {
                csv_ignore_set.emplace(lparts.back());
              }
              else {
                if (args.concept_scheme == "platforms") {
                  std::string platform_type;
                  for (const auto& re : search_re_list) {
                    std::string check_string;
                    for (size_t n=0; n < std::get<1>(re); ++n) {
                      if (!check_string.empty()) {
                        check_string+=",";
                      }
                      check_string+=lparts[n];
                    }
                    if (std::regex_search(check_string,std::get<2>(re))) {
                      platform_type=std::get<3>(re);
                      break;
                    }
                  }
                  csv_map.emplace(lparts.back(),std::make_tuple(lparts[last],path,platform_type));
                }
                else {
                  csv_map.emplace(lparts.back(),std::make_tuple(lparts[last],path,""));
                }
/*
                if (args.concept_scheme != "sciencekeywords" && lparts[last] == strutils::to_upper(lparts[last])) {
                  std::cerr << "Warning: '" << lparts[last] << "' is all upper-case" << std::endl;
                }
*/
              }
            }
            else {
              csv_ignore_set.emplace(lparts.back());
            }
          }
        }
        else {
          std::cerr << "SKIPPING DUPLICATE: " << lparts.back() << std::endl;
        }
      }
    }
  }
  std::string kms_url=KMS_API_URL+"/concepts/concept_scheme/";
  if (!args.alias.empty()) {
    kms_url+=args.alias;
  }
  else {
    kms_url+=args.concept_scheme;
  }
  kms_url+="?format=xml&page_num=";
  auto page_num=1;
  while (1) {
    unixutils::mysystem2("/bin/sh -c 'curl -k -s -S -o - \""+kms_url+strutils::itos(page_num)+"\"'",oss,ess);
    if (!ess.str().empty()) {
      std::cerr << "Error getting concepts: " << ess.str() << std::endl;
      exit(1);
    }
    auto idx=oss.str().find("<concepts");
    if (idx == std::string::npos) {
      if (page_num == 1) {
        std::cerr << "Error in concepts xml - unable to find <concepts> element" << std::endl;
        exit(1);
      }
      else {
        break;
      }
    }
    XMLSnippet concepts(oss.str().substr(oss.str().find("<concepts")));
    if (concepts) {
      std::string xpath="concepts/conceptBrief@conceptScheme="+args.concept_scheme;
      if (std::get<3>(args.concept_schemes[args.concept_scheme])) {
        auto elist=concepts.element_list(xpath+"@isLeaf=false");
        for (const auto& e : elist) {
          auto p=csv_map.find(e.attribute_value("uuid"));
          if (p != csv_map.end()) {
            csv_map.erase(p);
          }
        }
        xpath+="@isLeaf=true";
      }
      auto elist=concepts.element_list(xpath);
      if (elist.size() == 0) {
        std::cerr << "Aborting. /kms/concepts/concept_scheme/" << args.concept_scheme << "?format=xml may be corrupted." << std::endl;
        return;
      }
      for (const auto& e : elist) {
        auto p=csv_map.find(e.attribute_value("uuid"));
        if (p == csv_map.end()) {
          if (csv_ignore_set.find(e.attribute_value("uuid")) == csv_ignore_set.end()) {
            std::cerr << "MISSING UUID: " << e.attribute_value("uuid") << " (found '" << e.attribute_value("prefLabel") << "' in concepts, but not csv file)" << std::endl;
          }
        }
        else {
          std::string column_list,values_list;
          if (args.concept_scheme == "platforms") {
            column_list="(uuid,last_in_path,path,ObML_platformType,cflag,dflag)";
            values_list="'"+p->first+"','"+sql_ready(std::get<0>(p->second))+"','"+sql_ready(std::get<1>(p->second))+"','"+sql_ready(std::get<2>(p->second))+"',DEFAULT,'"+args.delete_flag+"'";
          }
          else {
            column_list="(uuid,last_in_path,path,cflag,dflag)";
            values_list="'"+p->first+"','"+sql_ready(std::get<0>(p->second))+"','"+sql_ready(std::get<1>(p->second))+"',DEFAULT,'"+args.delete_flag+"'";
          }
          std::string result;
          if (args.server.command("insert into search.GCMD_"+args.concept_scheme+" "+column_list+" values ("+values_list+") on duplicate key update cflag=if(cflag=9,9,if(last_in_path=values(last_in_path) and path=values(path),0,2)), last_in_path=values(last_in_path), path=values(path), dflag=values(dflag)",result) < 0) {
            std::cerr << "Error while inserting " << p->first << std::endl;
            std::cerr << args.server.error() << std::endl;
            exit(1);
          }
          csv_map.erase(p);
        }
      }
    }
    else {
      std::cerr << "Error parsing concepts: " << concepts.parse_error() << std::endl;
    }
    ++page_num;
  }
  std::string result;
  if (args.server.command("insert into search.GCMD_versions values ('"+args.concept_scheme+"','"+version+"','"+revision_date+"') on duplicate key update version=values(version), revision_date=values(revision_date)",result) < 0) {
    std::cerr << "Error updating version number" << std::endl;
    std::cerr << args.server.error() << std::endl;
    exit(1);
  }
  if (csv_map.size() > 0) {
    std::cout << "**Warning: " << csv_map.size() << " CSV keywords were not exposed by the API" << std::endl;
    for (const auto& e : csv_map) {
      std::cout << "    " << e.first << " " << std::get<1>(e.second) << std::endl;
    }
  }
  std::cout << "...update of '" << args.concept_scheme << "' complete at " << dateutils::current_date_time().to_string() << std::endl;
}

void clean_keywords()
{
  MySQL::LocalQuery query("select uuid,path from search.GCMD_"+args.concept_scheme+" where dflag != '"+args.delete_flag+"' and uuid not like 'RDA%'");
  if (query.submit(args.server) == 0) {
    if (query.num_rows() == 0) {
      std::cout << "No GCMD keywords were deleted in this update" << std::endl;
    }
    else {
      std::cout << query.num_rows() << " GCMD";
      if (query.num_rows() > 1) {
        std::cout << " keywords were";
      }
      else {
        std::cout << " keyword was";
      }
      std::cout << " deleted in this update:" << std::endl;
      MySQL::Row row;
      while (query.fetch_row(row)) {
        std::cout << "DELETED: " << row[0] << " '" << row[1] << "'" << std::endl;
      }
    }
  }
  else {
    std::cerr << "Error: unable to check for deleted keywords" << std::endl;
    std::cerr << query.error() << std::endl;
    exit(1);
  }
  args.server._delete("search.GCMD_"+args.concept_scheme,"dflag != '"+args.delete_flag+"' and uuid not like 'RDA%'");
}

void show_changed_keywords()
{
  MySQL::LocalQuery query("select uuid,path from search.GCMD_"+args.concept_scheme+" where cflag = 2 and uuid not like 'RDA%'");
  if (query.submit(args.server) == 0) {
    if (query.num_rows() == 0) {
      std::cout << "No GCMD keywords were changed in this update" << std::endl;
    }
    else {
      std::cout << query.num_rows() << " GCMD";
      if (query.num_rows() > 1) {
        std::cout << " keywords were";
      }
      else {
        std::cout << " keyword was";
      }
      std::cout << " changed in this update:" << std::endl;
      MySQL::Row row;
      while (query.fetch_row(row)) {
        std::cout << "CHANGED: " << row[0] << " '" << row[1] << "'" << std::endl;
      }
    }
  }
  else {
    std::cerr << "Error: unable to check for changed keywords" << std::endl;
    std::cerr << query.error() << std::endl;
    exit(1);
  }
  query.set("select uuid,path from search.GCMD_"+args.concept_scheme+" where cflag = 1 and uuid not like 'RDA%'");
  if (query.submit(args.server) == 0) {
    if (query.num_rows() == 0) {
      std::cout << "No GCMD keywords were added in this update" << std::endl;
    }
    else {
      std::cout << query.num_rows() << " GCMD";
      if (query.num_rows() > 1) {
        std::cout << " keywords were";
      }
      else {
        std::cout << " keyword was";
      }
      std::cout << " added in this update:" << std::endl;
      MySQL::Row row;
      while (query.fetch_row(row)) {
        std::cout << "ADDED: " << row[0] << " '" << row[1] << "'" << std::endl;
      }
    }
  }
  else {
    std::cerr << "Error: unable to check for added keywords" << std::endl;
    std::cerr << query.error() << std::endl;
    exit(1);
  }
}

void update_rda_keywords()
{
  for (auto p : args.rda_keywords) {
    if (p.second == 1) {
// add the keyword
      auto parts=strutils::split(p.first," > ");
      std::string result;
      if (args.server.command("insert into search.GCMD_"+args.concept_scheme+" (uuid,last_in_path,path,cflag,dflag) values ('RDA"+strutils::uuid_gen().substr(3)+"','"+sql_ready(parts.back())+"','"+sql_ready(p.first)+"',DEFAULT,'"+args.delete_flag+"') on duplicate key update cflag=if(cflag=9,9,if(last_in_path=values(last_in_path) and path=values(path),0,2)), last_in_path=values(last_in_path), path=values(path), dflag=values(dflag)",result) < 0) {
        std::cerr << "Error while inserting " << p.first << std::endl;
        std::cerr << args.server.error() << std::endl;
        exit(1);
      }
    }
  }
  MySQL::LocalQuery query("select uuid,path from search.GCMD_"+args.concept_scheme+" where cflag = 1 and dflag = '"+args.delete_flag+"' and uuid like 'RDA%'");
  if (query.submit(args.server) == 0) {
    if (query.num_rows() == 0) {
      std::cout << "No RDA keywords were added in this update" << std::endl;
    }
    else {
      std::cout << query.num_rows() << " RDA";
      if (query.num_rows() > 1) {
        std::cout << " keywords were";
      }
      else {
        std::cout << " keyword was";
      }
      std::cout << " added in this update:" << std::endl;
      MySQL::Row row;
      while (query.fetch_row(row)) {
        std::cout << "ADDED RDA KEYWORD: " << row[0] << " '" << row[1] << "'" << std::endl;
      }
    }
  }
  else {
    std::cerr << "Error: unable to check for added RDA keywords" << std::endl;
    std::cerr << query.error() << std::endl;
    exit(1);
  }
}

void show_orphaned_keywords()
{
  std::vector<std::tuple<std::string,std::string>> keyword_tables{
    std::make_tuple("variables","GCMD_sciencekeywords"),
    std::make_tuple("platforms_new","GCMD_platforms"),
    std::make_tuple("contributors_new","GCMD_providers"),
    std::make_tuple("instruments_new","gcmd_instruments"),
    std::make_tuple("projects_new","GCMD_projects"),
    std::make_tuple("supportedProjects_new","GCMD_projects")
  };
  for (const auto& map : keyword_tables) {
    MySQL::LocalQuery query("select distinct keyword,dsid from search."+std::get<0>(map)+" as k left join search."+std::get<1>(map)+" as g on g.uuid = k.keyword where k.vocabulary = 'GCMD' and isnull(g.uuid) order by dsid,keyword");
    if (query.submit(args.server) == 0) {
      for (const auto& row : query) {
        std::cout << "ORPHANED KEYWORD: " << row[0] << " in " << row[1] << " (" << std::get<0>(map) << ")" << std::endl;
      }
    }
    else {
      std::cerr << "Database error for query '" << query.show() << "': '" << query.error() << "'" << std::endl;
    }
  }
}

int main(int argc,char **argv)
{
  parse_args(argc,argv);
  if (!args.no_gcmd_update) {
    std::vector<std::string> concept_schemes;
    if (args.concept_scheme == "all") {
      for (const auto& s : args.concept_schemes) {
        concept_schemes.emplace_back(s.first);
      }
    }
    else {
      concept_schemes.emplace_back(args.concept_scheme);
    }
    for (const auto& s : concept_schemes) {
      args.concept_scheme=s;
      if (passed_date_check()) {
        update_keywords();
        clean_keywords();
        show_changed_keywords();
      }
    }
  }
  update_rda_keywords();
  if (!args.no_gcmd_update) {
    show_orphaned_keywords();
  }
}
