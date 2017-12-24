#include <string.h>
#include <stdio.h>
#include "stdlib.h"
#include <unistd.h>
#include <zmq.h>
#include <string.h>
#include <pthread.h>
#include <algorithm>
#include <vector>
#include <map>
#include <ctime>
#include <signal.h>

#define SOCKET_STRING "tcp://127.0.0.1:1102"
#define INTERNAL_WORKER_ADDRESS "inproc://worker"
#define WORKER_THREAD_COUNT 10

void *z_ctx;
bool flag_kill;

class Binds{
	private:
		std::map<std::string,std::string> binds; //привязка, id клиента к id рабочего
	public:
		int check_id(std::string id); //проверяет привязан ли клиент к какоу-либо рабочему, если нет, то привзывает к свободному
		void insert_workers(std::string worker_ID); //добавляем id рабочего в мапу рабочих
		std::string get_client_id(std::string worker_ID); //возвращает id клиента по id рабочего
		std::string get_worker_id(std::string id); //возвращает id рабочего по id клиента
		void set_worker_free(std::string worker_id); //освобождает рабочего, удаляет привязку к клиенту
};

void Binds::insert_workers(std::string worker_ID)
{
	if(binds.count(worker_ID)==0) {
		binds.insert(std::pair<std::string,std::string>(worker_ID,std::string("0")));
		printf("new worker insert to list with id = %s\n",worker_ID.c_str());
	}
}

std::string Binds::get_client_id(std::string worker_ID)
{
	return binds[worker_ID];
}

void Binds::set_worker_free(std::string worker_id)
{
	printf("client with id = %s disconnected\n",binds[worker_id].c_str());
	binds[worker_id]="0";
}

int Binds::check_id(std::string id)
{
	std::map<std::string,std::string>::iterator it;
	std::map<std::string,std::string>::iterator it1;
	for(it=binds.begin();it!=binds.end();it++)
	{	
		if(it->second==id) return 0;
		if(it->second=="0"){
			printf("client with id=%s connected\n",id.c_str());
			it->second=id;
			return 0;
		}
	}
	return -1;
}

std::string Binds::get_worker_id(std::string id)
{
	std::map<std::string,std::string>::iterator it;
	for(it=binds.begin();it!=binds.end();it++)	
		if(it->second==id){
			printf("clientID= %s, workerID= %s\n",id.c_str(),it->first.c_str());
			return it->first;}
	return std::string("0");
}

void *worker_thread(void *)
{
	int last=0;
  	srand(time(0)+rand() % 100);
  	char str[11];
	unsigned int next_numb;
	unsigned char numb[4];
	unsigned char *msg;
  	static const char alphanum[] =
  	"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  	"abcdefghijklmnopqrstuvwxyz";
	int stringLength = sizeof(alphanum) - 1;
	//создаем индентификатор сокета
  	for(unsigned int i = 0; i < 10; i++)
  	{
    		str[i] = alphanum[rand() % stringLength];
  	}
	std::vector<unsigned int> numbers;
	void* sock=zmq_socket(z_ctx,ZMQ_DEALER);
	if (!sock)
	{
		return NULL;
	}
	zmq_setsockopt(sock,ZMQ_IDENTITY, str, 10);
	int conn=zmq_connect(sock,INTERNAL_WORKER_ADDRESS);
	if (conn<0)
	{
		zmq_close(sock);
		return NULL;
	}
	//отправляем брокеру инициализирующее сообщение, чтобы он сохранил id рабочего
	zmq_send(sock,"HELLO",5,ZMQ_DONTWAIT);	
	zmq_pollitem_t poll_fd;
	poll_fd.socket=sock;
	poll_fd.events=ZMQ_POLLIN;
	poll_fd.revents=0;
	unsigned int tick(0);
	bool fl;
	while(!flag_kill)
	{
		//мониторим сокет, интервал 500мс
		int res = zmq_poll(&poll_fd, 1,500);
		if(res<0) break; 
		if(res==0) 
		{	
			//если в течении 10 секунд не было сообщений, отправлем брокеру просьбу освободить рабочего
			tick++;		
			if(tick>20 & fl) {
				zmq_send(sock,"SETMEFREE",9,ZMQ_DONTWAIT);
				numbers.clear();
				tick=0;fl=false;
			}
			continue;
		}
		if(poll_fd.revents & ZMQ_POLLIN)
		{
			poll_fd.revents &=~ZMQ_POLLIN;
			tick=0;fl=true;
			zmq_msg_t rcv_msg;
			if(zmq_msg_init(&rcv_msg)<0)
			{printf("msg init err\n");
				flag_kill=true;
				break;
			}
			//читаем пакет
			int rc=zmq_msg_recv(&rcv_msg,sock,ZMQ_DONTWAIT);
			if(rc<0)
			{printf("msg recv err\n");
				flag_kill=true;
				break;
			}
			int msg_size=zmq_msg_size(&rcv_msg);
			if(msg_size==5) {
				printf("heartbeat\n");
				tick=0;
				zmq_msg_close(&rcv_msg);
				continue;
			}
			//парсим пакет
			unsigned char *msg=(unsigned char*)zmq_msg_data(&rcv_msg);
			memcpy(numb,msg+1,sizeof(unsigned int));
			memcpy(&next_numb,(unsigned int*)numb,sizeof(unsigned int));
			numbers.push_back(next_numb);
			last++;
			//если тип пакета =1, то отправляем в ответе медиану
			if((unsigned int)msg[0]==49){
				std::sort(numbers.begin(),numbers.end());
				if(zmq_send(sock,&numbers[last/2],sizeof(unsigned int),ZMQ_DONTWAIT)<0)
				{
					printf("zmq_send() error\n");
					break;
				}
			}
			zmq_msg_close(&rcv_msg);
		}
	}
	zmq_close(sock);
	printf("worker with id = %s closed\n",str);
	return NULL;
}

void termination_handler(int)
{
	flag_kill=true;
}


int main (int argc, char const *argv[]) 
{
	signal(SIGTERM, termination_handler);
        signal(SIGSTOP, termination_handler);
        signal(SIGINT,  termination_handler);
	signal(SIGQUIT, termination_handler);
	z_ctx = zmq_ctx_new();
	if(!z_ctx) return 1;
	Binds ids;
	//сокет для килентов
	void* sock_srv=zmq_socket(z_ctx, ZMQ_ROUTER);
	if(!sock_srv)
	{
		zmq_ctx_destroy(z_ctx);
		return 2;
	}
	if(zmq_bind(sock_srv, SOCKET_STRING)<0)
	{
		zmq_close(sock_srv);
		zmq_ctx_destroy(z_ctx);
		return 3;
	}
	//сокет для рабочих
	void* sock_workers=zmq_socket(z_ctx, ZMQ_ROUTER);
	if(!sock_workers)
	{
		zmq_close(sock_srv);
		zmq_ctx_destroy(z_ctx);
		return 3;
	}
  	if (zmq_bind(sock_workers, INTERNAL_WORKER_ADDRESS) < 0)
  	{
   		zmq_close(sock_srv);
    		zmq_close(sock_workers);
    		zmq_ctx_destroy(z_ctx);
    		return 3;
  	}
	//создаем 10 рабочих потоков
	pthread_t worker_thread_ids[WORKER_THREAD_COUNT];
	for(int i=0;i<WORKER_THREAD_COUNT;i++)
	{
		pthread_create(&worker_thread_ids[i],NULL,&worker_thread,NULL);
	}
	
	zmq_pollitem_t poll_fds[2];
	poll_fds[0].socket=sock_srv;
	poll_fds[0].fd=0;
	poll_fds[0].events=ZMQ_POLLIN;
	poll_fds[0].revents=0;
	poll_fds[1].socket=sock_workers;
	poll_fds[1].fd=0;
	poll_fds[1].events=ZMQ_POLLIN;
	poll_fds[1].revents=0;
	zmq_msg_t rcv_msg;
	while(!flag_kill)
	{
		//начинаем мониторить сокеты, интервал 500 мс
		int res=zmq_poll(poll_fds,2,500);
		if(res<0) break;
		if(res==0) continue;
		if(poll_fds[0].revents & ZMQ_POLLIN)
		{
			poll_fds[0].revents&=~ZMQ_POLLIN;
			// читаем индентификатор клиента
			char str[11];
			int client_identity_len;
			client_identity_len=zmq_recv(sock_srv,&str,10,ZMQ_DONTWAIT|ZMQ_SNDMORE);
			std::string client_identity(str);
			if(client_identity_len<=0) break;
			// читаем данные
			if(zmq_msg_init(&rcv_msg)<0)
			{
				flag_kill=true;
				break;
			}
			int rc= zmq_msg_recv(&rcv_msg,sock_srv,ZMQ_DONTWAIT);
			if(rc<0)
			{
				flag_kill=true;
				break;
			}
			//проверяем привязан ли клиент к какому-либо рабочему, если нет то привязываем к свободному
			if(ids.check_id(client_identity)<0) 
			{
				zmq_msg_close(&rcv_msg); 
				continue;
			}
			char prom[11];
			strcpy(prom,ids.get_worker_id(client_identity).c_str());
			int msg_size=zmq_msg_size(&rcv_msg);
			//отправляем пакет рабочему
			if(zmq_send(sock_workers,&prom,10,ZMQ_DONTWAIT|ZMQ_SNDMORE)<0)
				break;
			if(zmq_send(sock_workers,zmq_msg_data(&rcv_msg),msg_size,ZMQ_DONTWAIT)<0)
				break;
			zmq_msg_close(&rcv_msg);
		}
		if(poll_fds[1].revents & ZMQ_POLLIN)
		{
			poll_fds[1].revents&=~ZMQ_POLLIN;
			//читаем идентификатор рабочего
			char str[11];
			int worker_identity_len;
			worker_identity_len=zmq_recv(sock_workers,str,10,ZMQ_DONTWAIT);
			std::string worker_identity(str);
			ids.insert_workers(worker_identity);
			if(worker_identity_len<=0) break;
			//читаем пакет
			if(zmq_msg_init(&rcv_msg)<0)
			{
				flag_kill=true;
				break;
			}
			int rc=zmq_msg_recv(&rcv_msg,sock_workers,ZMQ_DONTWAIT);
			if(rc<0)
			{
				flag_kill=true;
				break;
			}
			int msg_size=zmq_msg_size(&rcv_msg);
			//если длина сообщения от рабочего =9, то это просьба его освободить, освобождаем
			if(msg_size==9) {
				ids.set_worker_free(worker_identity);
				zmq_msg_close(&rcv_msg);
				continue;
			}
			char prom[11];
			strcpy(prom,ids.get_client_id(worker_identity).c_str());
			//если длина сообщения =4, это медиана, отправляем ее клиенту
			if (msg_size==4)
			{
				if(zmq_send(sock_srv,&prom,10,ZMQ_DONTWAIT|ZMQ_SNDMORE)<0)
					break;
				if(zmq_send(sock_srv,zmq_msg_data(&rcv_msg),msg_size,ZMQ_DONTWAIT)<0)
					break;		printf("send client\n");	
			}
			zmq_msg_close(&rcv_msg);
		}
	}
	usleep(6000);
		
	for(int i=0;i<WORKER_THREAD_COUNT;i++)
	{
		void *res=NULL;
		pthread_join(worker_thread_ids[i],&res);
	}
	zmq_close(sock_workers);
	zmq_close(sock_srv);
	zmq_ctx_destroy(z_ctx);
	return 0;
}
