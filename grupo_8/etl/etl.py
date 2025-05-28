import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError, DataError
from sqlalchemy.orm import sessionmaker
import logging
import traceback

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_errors.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('ETL')

from etl.abstract_etl import AbstractETL
from modelos.tb_proprietario import Proprietario
from modelos.tb_veiculo_registrado import VeiculoRegistrado
from modelos.tb_banco import Banco
from modelos.tb_empresa import Empresa
from modelos.tb_pessoa import Pessoa
from modelos.tb_caminhao import Caminhao
from modelos.tb_carro import Carro
from modelos.tb_dono import Dono


class ETL(AbstractETL):
    def __init__(self, origem, destino):
        super().__init__(origem, destino)
        self.engine = create_engine(destino)
        self.Session = sessionmaker(bind=self.engine)
        self._dados_transformados = {}

    def extract(self):
        try:
            self._dados_extraidos = pd.read_excel(self.origem, sheet_name=None)
        except FileNotFoundError as e:
            logger.error(f"[EXTRACTION ERROR] Arquivo não encontrado: {e}")
            logger.error(traceback.format_exc())
            raise
        except pd.errors.EmptyDataError as e:
            logger.error(f"[EXTRACTION ERROR] Arquivo vazio ou sem dados: {e}")
            logger.error(traceback.format_exc())
            raise
        except ValueError as e:
            logger.error(f"[EXTRACTION ERROR] Erro na leitura dos dados: {e}")
            logger.error(traceback.format_exc())
            raise
        except Exception as e:
            logger.error(f"[EXTRACTION ERROR] Erro inesperado: {e}")
            logger.error(traceback.format_exc())
            raise

    def transform(self):

        try:

            df = self._dados_extraidos['Banco']
            df['id_proprietario'] = pd.to_numeric(df['id_proprietario'], errors='coerce').astype('Int64')
            self._dados_transformados['Banco'] = df

            df = self._dados_extraidos['Caminhão']
            df['cod_veiculo'] = pd.to_numeric(df['cod_veiculo'], errors='coerce').astype('Int64')
            self._dados_transformados['Caminhão'] = df

            df = self._dados_extraidos['Carro']
            df['cod_veiculo'] = pd.to_numeric(df['cod_veiculo'], errors='coerce').astype('Int64')
            self._dados_transformados['Carro'] = df

            df = self._dados_extraidos['Dono']
            df['id_proprietario'] = pd.to_numeric(df['id_proprietario'], errors='coerce').astype('Int64')
            df['cod_veiculo'] = pd.to_numeric(df['cod_veiculo'], errors='coerce').astype('Int64')
            df['data_compra'] = pd.to_datetime(df['data_compra']).dt.date
            self._dados_transformados['Dono'] = df

            df = self._dados_extraidos['Empresa']
            df['id_proprietario'] = pd.to_numeric(df['id_proprietario'], errors='coerce').astype('Int64')
            self._dados_transformados['Empresa'] = df

            df = self._dados_extraidos['Pessoa']
            df['id_proprietario'] = pd.to_numeric(df['id_proprietario'], errors='coerce').astype('Int64')
            self._dados_transformados['Pessoa'] = df

            df = self._dados_extraidos['Proprietário']
            df['id_proprietario'] = pd.to_numeric(df['id_proprietario'], errors='coerce').astype('Int64')
            self._dados_transformados['Proprietário'] = df

            df = self._dados_extraidos['Veículo_Registrado']
            df['cod_veiculo'] = pd.to_numeric(df['cod_veiculo'], errors='coerce').astype('Int64')
            self._dados_transformados['Veículo_Registrado'] = df

        except KeyError as e:
            logger.error(f"[TRANSFORMATION ERROR] Coluna ausente no DataFrame: {e}")
            logger.error(traceback.format_exc())
            raise
        except ValueError as e:
            logger.error(f"[TRANSFORMATION ERROR] Erro de conversão de tipo: {e}")
            logger.error(traceback.format_exc())
            raise
        except TypeError as e:
            logger.error(f"[TRANSFORMATION ERROR] Erro de tipo: {e}")
            logger.error(traceback.format_exc())
            raise
        except Exception as e:
            logger.error(f"[TRANSFORMATION ERROR] Erro inesperado: {e}")
            logger.error(traceback.format_exc())
            raise


    
    def load(self):
        session = self.Session()
        try:
            lista_proprietario = []
            lista_proprietario.extend(Proprietario.from_dataframe(self._dados_transformados['Proprietário']))
            session.add_all(lista_proprietario)
            session.commit()

        # Veiculo registrado
            lista_veiculo_registrado = []
            lista_veiculo_registrado.extend(VeiculoRegistrado.from_dataframe(self._dados_transformados['Veículo_Registrado']))
            session.add_all(lista_veiculo_registrado)
            session.commit()

            # Banco
            lista_banco = []
            lista_banco.extend(Banco.from_dataframe(self._dados_transformados['Banco']))
            session.add_all(lista_banco)
            session.commit()

            # Empresa
            lista_empresa = []
            lista_empresa.extend(Empresa.from_dataframe(self._dados_transformados['Empresa']))
            session.add_all(lista_empresa)
            session.commit()

            # Pessoa
            lista_pessoa = []
            lista_pessoa.extend(Pessoa.from_dataframe(self._dados_transformados['Pessoa']))
            session.add_all(lista_pessoa)
            session.commit()

            # Caminhão      
            lista_caminhao = []
            lista_caminhao.extend(Caminhao.from_dataframe(self._dados_transformados['Caminhão']))
            session.add_all(lista_caminhao)
            session.commit()

            # Carro
            lista_carro = []
            lista_carro.extend(Carro.from_dataframe(self._dados_transformados['Carro']))
            session.add_all(lista_carro)
            session.commit()
            
            # Dono
            lista_dono = []
            lista_dono.extend(Dono.from_dataframe(self._dados_transformados['Dono']))
            session.add_all(lista_dono)
            session.commit()

            print("Dados carregados com sucesso.")
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Erro ao carregar dados: {e}")
        except IntegrityError as e:
            session.rollback()
            logger.error(f"[LOADING ERROR] Violação de integridade: {e}")
            logger.error(traceback.format_exc())
            raise
        except OperationalError as e:
            session.rollback()
            logger.error(f"[LOADING ERROR] Erro operacional: {e}")
            logger.error(traceback.format_exc())
            raise
        except DataError as e:
            session.rollback()
            logger.error(f"[LOADING ERROR] Erro de dados: {e}")
            logger.error(traceback.format_exc())
            raise
        except Exception as e:
            session.rollback()
            logger.error(f"[LOADING ERROR] Erro inesperado: {e}")
            logger.error(traceback.format_exc())
            raise
        finally:
            session.close()