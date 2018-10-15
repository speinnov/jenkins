def username_cicd = 'socgen'
echo 'Hello Mr. ${username}'
echo "I said, Hello Mr. ${username_cicd}"



pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                
                echo "Building ${env.BUILD_ID} on ${env.JENKINS_URL}  job ${JOB_NAME}"
            }
        }
        stage('Test') {
            steps {
                echo 'Testing..'
            }
        }
        stage('Deploy') {
            steps {
                echo 'Deploying....'
            }
        }
    }
}
