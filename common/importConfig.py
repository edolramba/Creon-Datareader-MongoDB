import configparser as parser

# 프로퍼티 파일 읽기
class importConfig:
    
    properties = parser.ConfigParser()
    properties.read('C:\Dev\Creon-Datareader\config\config.ini')
    
    ## 반환에 사용할 변수들 ##
    host = ""
    port = ""
    ## section 에 넣은 값에 따라 config 파일의 section 을 읽는다.
    def select_section(self, section):
        
        if(section == "MONGODB"):
            self.host=self.properties[section]['host']
            self.port=self.properties[section]['port']
            return {"host": self.host, "port": self.port}
            
        else:
            print("not yet setting section")