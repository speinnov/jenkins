# jenkins
cicd test with talend


# install Commandline
configuration of reporsitories:
<commandlinePath>/configuration/maven_user_settings.xml

# Nexus Continuous Integration repositories

**create the next repositories: 

talend-updates. (add patch in this repositoriy)
talend-custom-libs. (reserved for cicd)
talend-custom-libs-release (reserved for cicd)
talend-custom-libs-snapshot (reserved for cicd)

**Installing Talend CI Builder and uploading it on Nexus

1.	Unzip the Talend-CI-Builder-V6.4.1.zip  and load it in the **thirdparty** reporsitory on Nexus server (deleared in maven_user_settings.xml )
V6.4.1 is the talend release version.

mvn install:install-file -Dfile=ci.builder--6.4.1.jar -DpomFile=ci.builder-6.4.1.pom

2. deploy the new repository on Nexus
From the folder were  Talend-CI-Builder-V6.4.1.zip  was decompressed run

mvn deploy:deploy-file -Dfile=ci.builder-6.4.1.jar -DpomFile=ci.builder-6.4.1.pom -DrepositoryId=thirdparty -Durl=http://localhost:8081/repository/thirdparty/ -s <commandlinePath>/configuration/maven_user_settings.xml
  
  
  
