/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * Usage example. 
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */
package ch.ethz.microperf.benchmarks;

import ch.ethz.microperf.ThroughputHandler;
import ch.ethz.microperf.Utils;
import ch.ethz.rdma.RdmaChannel;
import ch.ethz.serializers.Serializer;

import com.ibm.disni.verbs.*;

import java.nio.ByteBuffer;
import java.util.LinkedList;

public class BenchmarkRDMA {
    public static double measureLatencyRDMA(RdmaChannel rdmachannel, int to_send) throws Exception {

        LinkedList<IbvSendWR> wrList_send = new LinkedList<IbvSendWR>();

        IbvSge sgeSend = new IbvSge();
        LinkedList<IbvSge> sgeList = new LinkedList<IbvSge>();
        sgeList.add(sgeSend);
        IbvSendWR sendWR = new IbvSendWR();
        sendWR.setWr_id(1001);
        sendWR.setSg_list(sgeList);
        sendWR.setOpcode(IbvSendWR.IBV_WR_SEND_WITH_IMM);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        sendWR.setImm_data(to_send);
        wrList_send.add(sendWR);

        SVCPostSend send = rdmachannel.ep.qp.postSend(wrList_send, null);

        LinkedList<IbvRecvWR> wrList_recv = new LinkedList<IbvRecvWR>();

        IbvSge sgeRecv = new IbvSge();
        LinkedList<IbvSge> sgeListRecv = new LinkedList<IbvSge>();
        sgeListRecv.add(sgeRecv);
        IbvRecvWR recvWR = new IbvRecvWR();
        recvWR.setSg_list(sgeListRecv);
        recvWR.setWr_id(1000);
        wrList_recv.add(recvWR);

        SVCPostRecv recv = rdmachannel.ep.qp.postRecv(wrList_recv, null);

        IbvWC[] wcListsend = new IbvWC[1];
        IbvWC[] wcListrecv = new IbvWC[1];
        wcListsend[0] = new IbvWC();
        wcListrecv[0] = new IbvWC();
        SVCPollCq pollsendCqCall = rdmachannel.attr.getSend_cq().poll(wcListsend, 1);
        SVCPollCq pollrecvCqCall = rdmachannel.attr.getRecv_cq().poll(wcListrecv, 1);


        // measure latency
        for(int i = 0; i < 500; i ++) {
            recv.execute();
            send.execute();
            while(pollsendCqCall.execute().getPolls()==0) {
                //empty
            }
            while(pollrecvCqCall.execute().getPolls()==0) {
                //empty
            }
        }

        // the test warm up
        long time = Utils.getTimeMicroseconds();
        for(int i = 0; i < 600; i ++) {
            recv.execute();
            send.execute();
            while(pollsendCqCall.execute().getPolls()==0) {
                //empty
            }
            while(pollrecvCqCall.execute().getPolls()==0) {
                //empty
            }
        }
        long total =  Utils.getTimeMicroseconds() - time;
        return (total) / 600.0;
    }

    public static int getDataSizeRDMA(RdmaChannel rdmachannel) throws Exception {

        LinkedList<IbvRecvWR> wrList_recv = new LinkedList<IbvRecvWR>();
        IbvSge sgeRecv = new IbvSge();
        LinkedList<IbvSge> sgeListRecv = new LinkedList<IbvSge>();
        sgeListRecv.add(sgeRecv);
        IbvRecvWR recvWR = new IbvRecvWR();
        recvWR.setSg_list(sgeListRecv);
        recvWR.setWr_id(1000);
        wrList_recv.add(recvWR);

        SVCPostRecv recv = rdmachannel.ep.qp.postRecv(wrList_recv, null);
        recv.execute();

        LinkedList<IbvSendWR> wrList_send = new LinkedList<IbvSendWR>();

        IbvSge sgeSend = new IbvSge();
        LinkedList<IbvSge> sgeList = new LinkedList<IbvSge>();
        sgeList.add(sgeSend);
        IbvSendWR sendWR = new IbvSendWR();
        sendWR.setWr_id(1001);
        sendWR.setSg_list(sgeList);
        sendWR.setOpcode(IbvSendWR.IBV_WR_SEND_WITH_IMM);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        sendWR.setImm_data(0);
        wrList_send.add(sendWR);

        SVCPostSend send = rdmachannel.ep.qp.postSend(wrList_send, null);


        IbvWC[] wcListsend = new IbvWC[1];
        IbvWC[] wcListrecv = new IbvWC[1];
        wcListsend[0] = new IbvWC();
        wcListrecv[0] = new IbvWC();
        SVCPollCq pollsendCqCall = rdmachannel.attr.getSend_cq().poll(wcListsend, 1);
        SVCPollCq pollrecvCqCall = rdmachannel.attr.getRecv_cq().poll(wcListrecv, 1);


        for(int i = 0; i < 1100 - 1; i ++) {
            while(pollrecvCqCall.execute().getPolls()==0) {
                //empty
            }
            recv.execute();
            send.execute();
            while(pollsendCqCall.execute().getPolls()==0) {
                //empty
            }
        }

        while(pollrecvCqCall.execute().getPolls()==0) {
            //empty
        }
        send.execute();
        while(pollsendCqCall.execute().getPolls()==0) {
            //empty
        }

        return wcListrecv[0].getImm_data();
    }


    public static void runLatencyTestSender(RdmaChannel rdmachannel, Object obj, Serializer serializerImpl, int iters) throws Exception {
        int object_length = serializerImpl.getObjectLength(obj);

        Thread.sleep(200);
        System.out.println(String.format("[Sender] sending %d bytes ", object_length));

        double latency = BenchmarkRDMA.measureLatencyRDMA(rdmachannel, object_length);
        System.out.println( "[Sender] Round trip latency : " + latency + " us");
        System.out.println( "-------------------------------------------\n");
        Thread.sleep(200);
        ByteBuffer direct_send_buffer = ByteBuffer.allocateDirect(object_length);

        int access = IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE | IbvMr.IBV_ACCESS_REMOTE_READ;
        IbvMr mr = rdmachannel.ep.pd.regMr(direct_send_buffer, access).execute().free().getMr();

        LinkedList<IbvSendWR> wrList_send = new LinkedList<IbvSendWR>();

        IbvSge sgeSend = new IbvSge();
        sgeSend.setAddr(mr.getAddr());
        sgeSend.setLength(mr.getLength());
        sgeSend.setLkey(mr.getLkey());
        LinkedList<IbvSge> sgeList = new LinkedList<IbvSge>();
        sgeList.add(sgeSend);
        IbvSendWR sendWR = new IbvSendWR();
        sendWR.setWr_id(1001);
        sendWR.setSg_list(sgeList);
        sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        wrList_send.add(sendWR);

        SVCPostSend send = rdmachannel.ep.qp.postSend(wrList_send, null);

        LinkedList<IbvRecvWR> wrList_recv = new LinkedList<IbvRecvWR>();

        IbvSge sgeRecv = new IbvSge();
        LinkedList<IbvSge> sgeListRecv = new LinkedList<IbvSge>();
        sgeListRecv.add(sgeRecv);
        IbvRecvWR recvWR = new IbvRecvWR();
        recvWR.setSg_list(sgeListRecv);
        recvWR.setWr_id(1000);
        wrList_recv.add(recvWR);

        SVCPostRecv recv = rdmachannel.ep.qp.postRecv(wrList_recv, null);

        IbvWC[] wcListsend = new IbvWC[1];
        IbvWC[] wcListrecv = new IbvWC[1];
        wcListsend[0] = new IbvWC();
        wcListrecv[0] = new IbvWC();
        SVCPollCq pollsendCqCall = rdmachannel.attr.getSend_cq().poll(wcListsend, 1);
        SVCPollCq pollrecvCqCall = rdmachannel.attr.getRecv_cq().poll(wcListrecv, 1);

        for (int i = 0; i < iters; i++) {
            recv.execute();
            long time1 = Utils.getTimeMicroseconds();
            direct_send_buffer.clear();
            serializerImpl.serializeObjectToBuffer(obj, direct_send_buffer);
            long time2 = Utils.getTimeMicroseconds();
            send.execute();
            while(pollsendCqCall.execute().getPolls()==0) {
                //empty
            }
            long time3 = Utils.getTimeMicroseconds();

            while(pollrecvCqCall.execute().getPolls()==0) {
                //empty
            }
            long time4 = Utils.getTimeMicroseconds();

            long deserialization = wcListrecv[0].getImm_data();

            System.out.println(String.format("[Sender] serialization took %d us ", time2 - time1));
            System.out.println(String.format("[Sender] send took %d us ", time3 - time2));
            System.out.println(String.format("[Receiver] receive took 0 us "));
            System.out.println(String.format("[Receiver] deserialization took %d us ", deserialization));
            System.out.println(String.format("[Total] total time %d us\n", (time4 - time1) ));
        }

    }

    public static void runLatencyTestReceiver(RdmaChannel rdmachannel, Serializer serializerImpl, int iters) throws Exception{
        int object_length = BenchmarkRDMA.getDataSizeRDMA(rdmachannel);
        System.out.println( "Recveiver Object length " + object_length);


        ByteBuffer recv_buffer = ByteBuffer.allocateDirect(object_length);
        int access = IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE | IbvMr.IBV_ACCESS_REMOTE_READ;
        IbvMr mr = rdmachannel.ep.pd.regMr(recv_buffer, access).execute().free().getMr();

        LinkedList<IbvRecvWR> wrList_recv = new LinkedList<IbvRecvWR>();

        IbvSge sgeRecv = new IbvSge();
        sgeRecv.setAddr(mr.getAddr());
        sgeRecv.setLength(mr.getLength());
        sgeRecv.setLkey(mr.getLkey());
        LinkedList<IbvSge> sgeListRecv = new LinkedList<IbvSge>();
        sgeListRecv.add(sgeRecv);
        IbvRecvWR recvWR = new IbvRecvWR();
        recvWR.setSg_list(sgeListRecv);
        recvWR.setWr_id(1000);
        wrList_recv.add(recvWR);

        SVCPostRecv recv = rdmachannel.ep.qp.postRecv(wrList_recv, null);

        recv.execute();

        LinkedList<IbvSendWR> wrList_send = new LinkedList<IbvSendWR>();
        IbvSge sgeSend = new IbvSge();
        LinkedList<IbvSge> sgeList = new LinkedList<IbvSge>();
        sgeList.add(sgeSend);
        IbvSendWR sendWR = new IbvSendWR();
        sendWR.setWr_id(1001);
        sendWR.setSg_list(sgeList);
        sendWR.setOpcode(IbvSendWR.IBV_WR_SEND_WITH_IMM);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        wrList_send.add(sendWR);

        IbvWC[] wcListsend = new IbvWC[1];
        IbvWC[] wcListrecv = new IbvWC[1];
        wcListsend[0] = new IbvWC();
        wcListrecv[0] = new IbvWC();
        SVCPollCq pollsendCqCall = rdmachannel.attr.getSend_cq().poll(wcListsend, 1);
        SVCPollCq pollrecvCqCall = rdmachannel.attr.getRecv_cq().poll(wcListrecv, 1);


        for (int i = 0; i < iters - 1; i++) {
            recv_buffer.position(0);
            while(pollrecvCqCall.execute().getPolls()==0) {
                //empty
            }
            long time1 = Utils.getTimeMicroseconds();
            Object o = serializerImpl.deserializeObjectFromBuffer(recv_buffer);
            long time2 = Utils.getTimeMicroseconds();

            sendWR.setImm_data((int)(time2 - time1));
            rdmachannel.ep.qp.postSend(wrList_send, null).execute();

            recv.execute();

            while(pollsendCqCall.execute().getPolls()==0) {
                //empty
            }
        }
        recv_buffer.position(0);
        while(pollrecvCqCall.execute().getPolls()==0) {
            //empty
        }
        long time1 = Utils.getTimeMicroseconds();
        Object o = serializerImpl.deserializeObjectFromBuffer(recv_buffer);
        long time2 = Utils.getTimeMicroseconds();

        sendWR.setImm_data((int)(time2 - time1));
        rdmachannel.ep.qp.postSend(wrList_send, null).execute();
        while(pollsendCqCall.execute().getPolls()==0) {
            //empty
        }


        System.out.println( "[Receiver] Test is done ");
    }

    public static void runThroughputTestSender(RdmaChannel rdmachannel, Object obj, Serializer serializerImpl, ThroughputHandler th) throws Exception {
        int object_length = serializerImpl.getObjectLength(obj);

        final int recv_size = 32;
        final int poll_size = 32;

        final int maxoutstanding = 256;
        final int maxsends = 128;
        final int signaleach = 32;


        Thread.sleep(200);
        System.out.println(String.format("[Sender] sending %d bytes ", object_length));

        double latency = BenchmarkRDMA.measureLatencyRDMA(rdmachannel, object_length);
        System.out.println( "[Sender] Round trip latency : " + latency + " us");
        System.out.println( "-------------------------------------------\n");
        Thread.sleep(2000);
        ByteBuffer direct_send_buffer = ByteBuffer.allocateDirect(object_length);

        int access = IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE | IbvMr.IBV_ACCESS_REMOTE_READ;
        IbvMr mr = rdmachannel.ep.pd.regMr(direct_send_buffer, access).execute().free().getMr();

        LinkedList<IbvSendWR> wrList_send_signal = new LinkedList<IbvSendWR>();
        LinkedList<IbvSendWR> wrList_send_unsignal = new LinkedList<IbvSendWR>();

        IbvSge sgeSend = new IbvSge();
        sgeSend.setAddr(mr.getAddr());
        sgeSend.setLength(mr.getLength());
        sgeSend.setLkey(mr.getLkey());
        LinkedList<IbvSge> sgeList = new LinkedList<IbvSge>();
        sgeList.add(sgeSend);
        IbvSendWR sendWR = new IbvSendWR();
        sendWR.setWr_id(1001);
        sendWR.setSg_list(sgeList);
        sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        wrList_send_signal.add(sendWR);
        SVCPostSend send_signal = rdmachannel.ep.qp.postSend(wrList_send_signal, null);

        sendWR = new IbvSendWR();
        sendWR.setWr_id(1001);
        sendWR.setSg_list(sgeList);
        sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
        sendWR.setSend_flags(0);

        wrList_send_unsignal.add(sendWR);
        SVCPostSend send_unsignal = rdmachannel.ep.qp.postSend(wrList_send_unsignal, null);

        LinkedList<IbvRecvWR> wrList_recv = new LinkedList<IbvRecvWR>();

        IbvSge sgeRecv = new IbvSge();
        LinkedList<IbvSge> sgeListRecv = new LinkedList<IbvSge>();
        sgeListRecv.add(sgeRecv);
        IbvRecvWR recvWR = new IbvRecvWR();
        recvWR.setSg_list(sgeListRecv);
        recvWR.setWr_id(1000);
        wrList_recv.add(recvWR);

        SVCPostRecv recv = rdmachannel.ep.qp.postRecv(wrList_recv, null);
        for(int i = 0; i < recv_size; i++){
            recv.execute();
        }

        IbvWC[] wcListsend = new IbvWC[poll_size];
        for(int i =0; i < poll_size; i++) {
            wcListsend[i] = new IbvWC();
        }
        SVCPollCq pollCqCall = rdmachannel.attr.getSend_cq().poll(wcListsend, poll_size); // send cq MUST be equal to recv cq


        // warm up
        long startNs = System.nanoTime();
        long currentNs = startNs;
        long endtime = startNs + th.getWarmupNs();
        long iter = 0;

        int outstanding = 0;
        int sends = 0;


        while(endtime > currentNs){
            int events = pollCqCall.execute().getPolls();
            if(events < 0) {
                System.out.println("failed to poll in RDMA");
            }
            for(int i = 0; i < events; i++){
                if(wcListsend[i].getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()){
                    System.out.println("failed op in RDMA");
                    return;
                }
                if(wcListsend[i].getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_SEND.getOpcode()){
                    // it is send completion
             //       System.out.println("send compl");
                    sends -= signaleach;
                } else if (wcListsend[i].getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_RECV.getOpcode()){
                    // received ack
                //    System.out.printf("recv compl with %d \n",wcListsend[i].getImm_data());
                    outstanding -= wcListsend[i].getImm_data();
                    recv.execute();
                } else {
                    System.out.println("unknown compl");
                }
            }

            if(outstanding < maxoutstanding && sends < maxsends) {
                outstanding++;
                sends++;
                direct_send_buffer.clear();
                serializerImpl.serializeObjectToBuffer(obj, direct_send_buffer);
                iter++;
                if(iter % signaleach == 0) {
                    send_signal.execute();
                } else{
                    send_unsignal.execute();
                }
                currentNs = th.sleepAndGetTime(iter, startNs);
            } else {
                if(outstanding >= maxoutstanding){
                //    System.out.printf("Cannot sent because of outstanding limit ");
                }
                if(sends>=maxsends){
              //      System.out.printf("Cannot sent because of send limit ");
                }
            }
        }

        System.out.println(String.format("[Sender] Warmup is done. it took %d us", (currentNs - startNs) / 1000L ));

        startNs = System.nanoTime();
        currentNs = startNs;
        endtime = startNs + th.getTestNs();
        iter = 0;

        long completed = 0;

        while(endtime > currentNs){

            int events = pollCqCall.execute().getPolls();
            if(events < 0) {
                System.out.println("failed to poll in RDMA");
            }
            if(events>0){
       //         System.out.printf("got %d events\n",events);
            }
            for(int i = 0; i < events; i++){
                if(wcListsend[i].getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()){
                    System.out.println("failed op in RDMA");
                    return;
                }
                if(wcListsend[i].getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_SEND.getOpcode()){
                    // it is send completion
              //      System.out.printf("send completion \n");
                    sends -= signaleach;
                } else if (wcListsend[i].getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_RECV.getOpcode()){
                    // received ack
               //     System.out.printf("recv completion with %d \n",wcListsend[i].getImm_data());
                    outstanding -= wcListsend[i].getImm_data();
                    completed += wcListsend[i].getImm_data();
                    recv.execute();
                } else {
                    System.out.printf("unknown completion \n");
                }
            }

            if(outstanding < maxoutstanding && sends < maxsends) {
                outstanding++;
                sends++;
                direct_send_buffer.clear();
                serializerImpl.serializeObjectToBuffer(obj, direct_send_buffer);
                iter++;
                if(iter % signaleach == 0) {
                    send_signal.execute();
        //            System.out.printf("send signal \n");
                } else{
                    send_unsignal.execute();
           //         System.out.printf("send unsignal \n");
                }
                currentNs = th.sleepAndGetTime(iter, startNs);
            } else {
                if(outstanding >= maxoutstanding){
       //             System.out.printf("Cannot sent because of outstanding limit \n");
                }
                if(sends>=maxsends){
       //             System.out.printf("Cannot sent because of send limit \n");
                }
            }
        }

        System.out.println("wait for completion of last requests.");

        // wait for completion of last requests.
        while(sends >= signaleach) {
            int events = pollCqCall.execute().getPolls();
            if (events < 0) {
                System.out.println("failed to poll in RDMA");
            }
    //        if(events>0){
      //          System.out.printf("got %d events\n",events);
     //       }
            for (int i = 0; i < events; i++) {
                if (wcListsend[i].getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
                    System.out.println("failed op in RDMA");
                    return;
                }
                if (wcListsend[i].getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_SEND.getOpcode()) {
                    // it is send completion
          //          System.out.printf("send completion \n");
                    sends -= signaleach;
                } else if (wcListsend[i].getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_RECV.getOpcode()) {
                    // received ack
               //     System.out.printf("recv completion \n");
                    outstanding -= wcListsend[i].getImm_data();
                    completed += wcListsend[i].getImm_data();
                } else {
                    System.out.printf("unknown event\n");
                }
            }
        }
        currentNs = System.nanoTime();

        System.out.println(String.format("[Sender] Test is done. it took %d us, did %d iters", (currentNs - startNs) / 1000L ,completed));
        System.out.println(String.format("[Sender] achieved throughput is %.2f op/sec", (completed*1000.0F / ((currentNs - startNs) / 1_000_000.0F ) ) ));

        LinkedList<IbvSendWR> wrList_end = new LinkedList<IbvSendWR>();

        sendWR = new IbvSendWR();
        sendWR.setWr_id(1001);
        sendWR.setNum_sge(0);
        sendWR.setOpcode(IbvSendWR.IBV_WR_RDMA_WRITE_WITH_IMM);
        sendWR.setSend_flags(0);
        wrList_end.add(sendWR);

        SVCPostSend sendend = rdmachannel.ep.qp.postSend(wrList_end, null);
        sendend.execute();

    }


    public static void runThroughputTestReceiver(RdmaChannel rdmachannel, Serializer serializerImpl) throws Exception{
        int object_length = BenchmarkRDMA.getDataSizeRDMA(rdmachannel);

        final int ackeach = 16;
        final int recvbuffers = 256;

        ByteBuffer[] recv_buffers = new ByteBuffer[recvbuffers];
        IbvMr[] mrs = new IbvMr[recvbuffers];
        SVCPostRecv[] recvs = new SVCPostRecv[recvbuffers];

        for(int i =0; i < recvbuffers; i++){
            recv_buffers[i] = ByteBuffer.allocateDirect(object_length);
            int access = IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE | IbvMr.IBV_ACCESS_REMOTE_READ;
            mrs[i] = rdmachannel.ep.pd.regMr(recv_buffers[i], access).execute().free().getMr();

            LinkedList<IbvRecvWR> wrList_recv = new LinkedList<IbvRecvWR>();
            IbvSge sgeRecv = new IbvSge();
            sgeRecv.setAddr(mrs[i].getAddr());
            sgeRecv.setLength(mrs[i].getLength());
            sgeRecv.setLkey(mrs[i].getLkey());
            LinkedList<IbvSge> sgeListRecv = new LinkedList<IbvSge>();
            sgeListRecv.add(sgeRecv);
            IbvRecvWR recvWR = new IbvRecvWR();
            recvWR.setSg_list(sgeListRecv);
            recvWR.setWr_id(i);
            wrList_recv.add(recvWR);

            recvs[i] = rdmachannel.ep.qp.postRecv(wrList_recv, null);
            recvs[i].execute();
       //     System.out.printf("Post recv %d length",mrs[i].getLength());
        }


        LinkedList<IbvSendWR> wrList_send = new LinkedList<IbvSendWR>();
        IbvSendWR sendWR = new IbvSendWR();
        sendWR.setWr_id(1001);
        sendWR.setOpcode(IbvSendWR.IBV_WR_SEND_WITH_IMM);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        sendWR.setNum_sge(0);
        sendWR.setImm_data(ackeach);
        wrList_send.add(sendWR);

        SVCPostSend send_ack = rdmachannel.ep.qp.postSend(wrList_send, null);


        IbvWC[] wcListsend = new IbvWC[32];
        for(int i =0; i < 32; i++){
            wcListsend[i] = new IbvWC();
        }
        SVCPollCq pollCqCall = rdmachannel.attr.getSend_cq().poll(wcListsend, 32);

        int counter = 0;
        int sends = 0;

        boolean not_done = false;
        System.out.println("Receiver: ready for test");

        while(!not_done){

            int events = pollCqCall.execute().getPolls();
            if(events < 0){
                System.out.println("Failed to poll in RDMA");
            }
            if(events>0){
          //      System.out.printf("recver:  Got events %d \n",events);
            }
            for(int i =0 ; i < events; i++){
                if (wcListsend[i].getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
                    System.out.println("failed op in RDMA");
                    return;
                }
                if (wcListsend[i].getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_SEND.getOpcode()) {
                    // it is send completion
            //        System.out.printf("recver: send compl \n");
                    sends--;
                } else if (wcListsend[i].getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_RECV.getOpcode()) {
                    // received data
            //        System.out.printf("recver: recv compl \n");
                    counter++;

                    int bufferid = (int)wcListsend[i].getWr_id();
                    recv_buffers[bufferid].position(0);
                    Object o = serializerImpl.deserializeObjectFromBuffer(recv_buffers[bufferid]);
                    recvs[bufferid].execute();

                    if(counter > ackeach) {
            //            System.out.printf("recver: send ack \n");
                        send_ack.execute();
                        counter -= ackeach;
                        sends++;
                    }
                } else if (wcListsend[i].getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_RECV_RDMA_WITH_IMM.getOpcode()) {
                    // completion of test
                    System.out.printf("recver: test completion \n");
                    not_done = true;
                } else {
                    System.out.printf("recver: uknnown %d \n",wcListsend[i].getOpcode() );
                }
            }
        }


        System.out.println( "[Receiver] Test is done ");
    }


}
