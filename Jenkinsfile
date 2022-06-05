pipeline {
    agent any

    triggers {
        githubPush()
    }

    stages {
        stage('Prepare') {
            steps {
                deleteDir()
                git 'https://github.com/idfortytwo/pscheduler-backend'
            }
        }
        stage('Archive') {
            steps {
                sh 'zip sources.zip -r pscheduler/* requirements.txt -x *test*.* -x "**/__pycache__/*"'
            }

            post {
                success {
                    archiveArtifacts 'sources.zip'
                }
            }
        }
    }
}
