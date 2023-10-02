primeiro apos o login criaremos um usuário IAM com acesso programatico  
com permissão de administrator access e baixe o CSV
após a criação precisaremos instalar o AWS CLI (linha de comando AWS)
apos a instalação no terminal executar o comando 'aws configure' para cofigurar suas credenciais de acesso do novo usuário
na configuração apos colocar aws_access_key_id e aws_secret_access_key
colocar a regiao defaut us-est-1
e o formato de saida como json

abrir o EMR na aws e criar o cluster
clicar em opções avançadas 
no release obter a ultima versão do EMR
e selecionar somente o hadoop e Spark
as outras opçoes ficam default e clique em next para as configuraçoes de hardware
selecionar a VPC default
EC2 public
apos a segurança de rede escolher o tipo da maquina, o que irá depender do projeto
a master - responsavel pelo gerenciamento do cluster
core - onde será feito o processamento pesado
auto termination é importante para gerenciar o custo
no proximo colocamos o nome do cluster
desmarcar o termination protection para conseguir terminar o cluster
e deixar tudo default
em opçoes de segurança deixar visivels para todas as contas IAM

depois de criado tudo no S#
vamos no airflow e cadastrar as variaveis de ambiente no nosso airflow em nossa maquina        
    admin>varebles
        aws_secret_access_key
        aws_access_key_id