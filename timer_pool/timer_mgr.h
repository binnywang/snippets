/// @file timer_mgr.h
/// @brief timer weel定时器实现，考虑到实用性和实现简单，采用单级time wheel.
/// 定时精度为1ms, 最长定时为1小时，可以通过修改kTimerBucketNum来支持更长的定时时间
/// 支持重复定时，支持存放在共享内存当中，重启后定时器可继续工作。
/// 定时回调函数需实现OnTimeOut接口，特别需要注意的是：尽量减少在回调函数中删除定时器，由于重入问题可能会影响定时器的正确性。
/// @author binny
/// @version 1.0
/// @date 2016-01-08
/// 
#ifndef TIMER_MGR_H_
#define TIMER_MGR_H_
#include <sys/shm.h>
#include <time.h>
#include <sys/time.h>
#include <stddef.h>
#include <stdint.h>
#include <assert.h>
#include <stdio.h>
#include <vector>

#define LOG_ERROR(format, ...)  printf(format, ##__VA_ARGS__)

template <typename TimerDataType>
class TimerMgr {
    public:
        /// 默认定时(0, 60*60*1000]
        static const size_t kTimerBucketNum = 60;

        typedef union TimerID {
            uint64_t id;
            struct {
                uint32_t pos;
                uint32_t seq;
            };
        } TimerID;

        struct TimerBucket {
            size_t head;
        };

        struct TimerObj {
            size_t prev;
            size_t next;
            int used;
            
            int interval_ms;
            int max_fire_count;
            int fire_count;
            int64_t expire;

            TimerID timer_id_obj;
            TimerDataType timer_data;
        };

        struct TimerHead {
            size_t max_size; 
            size_t data_size;
            size_t max_num;
            size_t used_num;
            size_t cur_bucket_pos;
            uint64_t cur_bucket_time;
            size_t free_head; /// 空闲链表头
            uint32_t seq;
        };

    public:
        TimerMgr() : timer_head_(NULL), timer_bucket_(NULL), timer_obj_(NULL) {
        }

        virtual ~TimerMgr() {
        }

        int Init(key_t key, size_t timer_num);

        int Init(void* mem, size_t max_size, size_t timer_num, bool fresh = true);

        static size_t TotalMemSize(size_t timer_num) {
            return (sizeof(TimerHead) + sizeof(TimerBucket) * kTimerBucketNum +  (timer_num + 1) * sizeof(TimerObj));
        }

        int AddTimer(int interval_ms, int fire_count, const TimerDataType& TimerData, uint64_t& timer_id);
        int DelTimer(const uint64_t& timer_id);

        int GetExpireTime(const uint64_t& timer_id, uint64_t& expire_time) const;

        int Update();

        size_t Size() const {
            return timer_head_->used_num;
        }

        size_t Capacity() const {
            return timer_head_->max_num;
        }

        const char* ErrMsg() const {
            return error_msg_;
        }
    public:
        virtual void OnTimeout(const uint64_t& timer_id, const TimerDataType& timer_data) = 0;

    private:

        size_t AllocNode() {
            size_t position = 0;
            if (timer_head_->free_head > 0 && timer_head_->used_num <= timer_head_->max_num) {
                position = timer_head_->free_head;
                timer_head_->free_head = timer_obj_[position].next;

                timer_head_->used_num++;
                timer_obj_[position].used = 1;

                timer_obj_[position].prev = 0;
                timer_obj_[position].next = 0;
            }
            return position;
        }
    
        void FreeNode(size_t position) {
            timer_obj_[position].used = 0;
            timer_obj_[position].prev = 0;
            timer_obj_[position].next = timer_head_->free_head;
            timer_head_->free_head = position;
            timer_head_->used_num--;
        }

    private:
        TimerHead* timer_head_;
        TimerBucket* timer_bucket_;
        TimerObj*   timer_obj_;
        char error_msg_[256];
};

template <typename TimerDataType>
int TimerMgr<TimerDataType>::Init(key_t key, size_t timer_num) {

    size_t max_size = TotalMemSize(timer_num); 
    if (key == 0) {
        char* mem = new char[max_size];
        return Init(mem, max_size, timer_num);
    } else {
        /// TODO: get shared memory
    }

    return 0;
}

template <typename TimerDataType> 
int TimerMgr<TimerDataType>::Init(void* mem, size_t max_size, size_t timer_num, bool fresh) {
    if (mem == NULL || max_size < TotalMemSize(timer_num)) {
        snprintf(const_cast<char*>(error_msg_), sizeof(error_msg_), "TimerMgr.Init mem size error, need total_size = %zu, but max_size =%zu", TotalMemSize(timer_num), max_size);
        return -1;
    }

    if (fresh) {
        timer_head_ = reinterpret_cast<TimerHead*>(mem);
        timer_head_->max_size = max_size;
        timer_head_->data_size = sizeof(TimerDataType);
        timer_head_->used_num = 0;
        timer_head_->max_num = timer_num;
        timer_head_->seq = (uint32_t)time(NULL);
        struct timeval now;
        gettimeofday(&now, NULL);
        ///timer_head_->cur_bucket_time = now.tv_sec * 1000 + now.tv_usec / 1000 ; 
        timer_head_->cur_bucket_time = now.tv_sec;
        timer_head_->cur_bucket_pos = timer_head_->cur_bucket_time % kTimerBucketNum;

        timer_head_->free_head = 0;

        timer_bucket_ = reinterpret_cast<TimerBucket*>((char*)mem + sizeof(TimerHead));
        for (size_t i = 0; i < kTimerBucketNum; i++) {
            timer_bucket_[i].head = 0;
        }

        timer_obj_ = reinterpret_cast<TimerObj*>((char*)mem + sizeof(TimerHead) +  kTimerBucketNum * sizeof(TimerBucket));
        for (size_t i = timer_num; i > 0; i--) {
            timer_obj_[i].prev = 0;
            timer_obj_[i].next = timer_head_->free_head;
            timer_head_->free_head = i;
        }

    } else {
        timer_head_ = reinterpret_cast<TimerHead*>(mem);
        if (timer_head_->max_size != max_size) {
            snprintf(error_msg_, sizeof(error_msg_), "TimerMgr.Init error, max_size[%zu, %zu] mismatch", timer_head_->max_size, max_size);
            return -1;
        }

         if (timer_head_->data_size != sizeof(TimerDataType)) {
            snprintf(error_msg_, sizeof(error_msg_), "TimerMgr.Init error, data_size[%zu, %zu] mismatch", timer_head_->data_size, sizeof(TimerDataType));
            return -2;
        }

        if(timer_head_->max_num != timer_num) {
            snprintf(error_msg_, sizeof(error_msg_), "TimerMgr.Init error, timer_num[%zu, %zu] mismatch", timer_head_->max_num, timer_num);
            return -3;
        }

        timer_bucket_ = reinterpret_cast<TimerBucket*>((char*)mem + sizeof(TimerHead));
        timer_obj_ = reinterpret_cast<TimerObj*>((char*)mem + sizeof(TimerHead) +  kTimerBucketNum * sizeof(TimerBucket));
    }

    return 0;
}

template <typename TimerDataType>
int TimerMgr<TimerDataType>::AddTimer(int interval_ms, int fire_count, const TimerDataType& timer_data, uint64_t& timer_id) {

    if (interval_ms <= 0 || interval_ms > (int)kTimerBucketNum || fire_count < 0) {
        snprintf(const_cast<char*>(error_msg_), sizeof(error_msg_), "TimerMgr.AddTimer error,invalide param: interval_ms = %d, fire_count = %d",
            interval_ms, fire_count);
        return -1;
    }

    struct timeval now;
    gettimeofday(&now, NULL);

   /// uint64_t expire = now.tv_sec * 1000 + now.tv_usec / 1000 + interval_ms; 
    uint64_t expire = now.tv_sec + interval_ms;
    if (expire < timer_head_->cur_bucket_time) {
        snprintf(const_cast<char*>(error_msg_), sizeof(error_msg_), "TimerMgr.AddTimer error, expire_time = %lu len than cur_bucket_time = %lu",
            expire, timer_head_->cur_bucket_time);
        return -1;
    }
    size_t position  = AllocNode();
    if (position == 0) {
        snprintf(const_cast<char*>(error_msg_), sizeof(error_msg_), "TimerMgr.AddTimer error, not enough timer node");
        return -2;
    }

    ///timer_obj_[position].next = 0;
    timer_obj_[position].timer_id_obj.pos = position;
    timer_obj_[position].timer_id_obj.seq = timer_head_->seq++;
    timer_obj_[position].interval_ms = interval_ms;
    timer_obj_[position].expire = expire; 
    timer_obj_[position].max_fire_count = fire_count;
    timer_obj_[position].fire_count = 0;
    timer_obj_[position].timer_data = timer_data;

    TimerBucket* bucket = timer_bucket_ + timer_obj_[position].expire % kTimerBucketNum;
    timer_obj_[position].prev = 0;
    timer_obj_[position].next = bucket->head;
    if (bucket->head) {
        timer_obj_[bucket->head].prev = position;
    }
    bucket->head = position;
    
    timer_id = timer_obj_[position].timer_id_obj.id;
    
    return 0;
}

template <typename TimerDataType>
int TimerMgr<TimerDataType>::DelTimer(const uint64_t& timer_id) {
	TimerID timer_id_obj;
	timer_id_obj.id = timer_id;

    if (timer_id_obj.pos <= 0 || timer_id_obj.pos > timer_head_->max_num)
    {
        snprintf(const_cast<char*>(error_msg_), sizeof(error_msg_), "TimerMgr.DelTimer error, invalid timer id: timer pos = %d",
            timer_id_obj.pos);
        return -1;
    }

    if (timer_obj_[timer_id_obj.pos].timer_id_obj.id != timer_id_obj.id || !timer_obj_[timer_id_obj.pos].used) {
        snprintf(const_cast<char*>(error_msg_), sizeof(error_msg_), "TimerMgr.DelTimer error, invalid timer id = %lu, inner timer node id = %lu",
            timer_id_obj.id, timer_obj_[timer_id_obj.pos].timer_id_obj.id);
        return -1;
    }


    TimerBucket* bucket = timer_bucket_ + timer_obj_[timer_id_obj.pos].expire % kTimerBucketNum;
    size_t next = bucket->head;

    while (next > 0) {
        if (timer_obj_[next].timer_id_obj.id == timer_id_obj.id) {
            break;
        }
        next = timer_obj_[next].next;       
    }

    if (next > 0) {
        if(timer_obj_[next].prev == 0) {
            bucket->head = timer_obj_[next].next;
        } else {
            timer_obj_[timer_obj_[next].prev].next = timer_obj_[next].next;
        }

        if (timer_obj_[next].next) {
            timer_obj_[timer_obj_[next].next].prev = timer_obj_[next].prev;
        }

        FreeNode(next);
        return 0;
    }
    snprintf(const_cast<char*>(error_msg_), sizeof(error_msg_), "TimerMgr.DelTimer error, cannot find timer_id=%lu", timer_id);
    return -1;
}

template <typename TimerDataType>
int TimerMgr<TimerDataType>::Update() {
    struct timeval now;
    gettimeofday(&now, NULL);
   // uint64_t now_ms = now.tv_sec * 1000 + now.tv_usec / 1000; 
    uint64_t now_ms = now.tv_sec;
    if (timer_head_->used_num <= 0) {
        timer_head_->cur_bucket_time = now_ms;
        timer_head_->cur_bucket_pos = now_ms % kTimerBucketNum;
        return 0;
    }

    /// 如果定时器没有及时update, 如两次update间隔大于1小时，需要把所有到期的定时器都执行一遍，因此这里不需要模kTimerBucketNum
    int wheel_count = now_ms - timer_head_->cur_bucket_time;
    for (int i = 1; i <= wheel_count; i++) {
        size_t pos = timer_bucket_[(timer_head_->cur_bucket_pos + i) % kTimerBucketNum].head;
        size_t next = 0;
        while (pos > 0) {
            /// 先保存指向下一个节点
            next = timer_obj_[pos].next;

            /// 执行回调函数
            timer_obj_[pos].fire_count++;
            OnTimeout(timer_obj_[pos].timer_id_obj.id , timer_obj_[pos].timer_data);

            /// OnTimeout中没有删除自己
            if (timer_obj_[pos].used) {

                /// 如果节点没有被释放，再次更新一下下个节点
                next = timer_obj_[pos].next;

                /// 从链表中取出当前节点
                if (timer_obj_[pos].prev) {
                    timer_obj_[timer_obj_[pos].prev].next = timer_obj_[pos].next;
                    if (timer_obj_[pos].next) {
                        timer_obj_[timer_obj_[pos].next].prev = timer_obj_[pos].prev;
                    }
                } else {
                    timer_bucket_[(timer_head_->cur_bucket_pos + i) % kTimerBucketNum].head = timer_obj_[pos].next;
                    if (timer_obj_[pos].next) {
                        timer_obj_[timer_obj_[pos].next].prev = 0;
                    }
                }

                /// 定时次数到了，释放节点
                if (timer_obj_[pos].max_fire_count != 0 &&
                    timer_obj_[pos].fire_count >= timer_obj_[pos].max_fire_count) {
                    FreeNode(pos);
                } else {
                    /// 再次定时
                    timer_obj_[pos].expire = now_ms + timer_obj_[pos].interval_ms;
                    TimerBucket* bucket = timer_bucket_ + timer_obj_[pos].expire % kTimerBucketNum;
                    timer_obj_[pos].prev = 0;
                    timer_obj_[pos].next = bucket->head;
                    if (bucket->head) {
                        timer_obj_[bucket->head].prev = pos;
                    }
                    bucket->head = pos;
                }
            }

            /// NOTE:如果在OnTimeout下个节点都被释放了，则链表被中断了，无法继续遍历,可能会导致后续定时器（如果有）未被正确的timeout, 需要再等1个小时
            if (next && !timer_obj_[next].used) {  
                LOG_ERROR("del timer in OnTimeout, maybe some timer was not correctly timeout, which will be timeout 1 hour later\n");
                continue;
            }
            pos = next;
        }
    }

    timer_head_->cur_bucket_pos = now_ms % kTimerBucketNum;
    timer_head_->cur_bucket_time = now_ms;

    return 0;
}

template <typename TimerDataType>
int TimerMgr<TimerDataType>::GetExpireTime(const uint64_t& timer_id, uint64_t& expire_time) const {
	TimerID timer_id_obj;
	timer_id_obj.id = timer_id;
    if (timer_id_obj.pos <= 0 || timer_id_obj.pos > timer_head_->max_num)
    {
        snprintf(const_cast<char*>(error_msg_), sizeof(error_msg_), "TimerMgr.GetExpireTime error, invalid timer id: timer pos = %d",
            timer_id_obj.pos);
        return -1;
    }

    if (timer_obj_[timer_id_obj.pos].timer_id_obj.id != timer_id|| !timer_obj_[timer_id_obj.pos].used) {
        snprintf(const_cast<char*>(error_msg_), sizeof(error_msg_), "TimerMgr.GetExpireTimes error, invalid timer id = %lu inner timer node id = %lu",
            timer_id, timer_obj_[timer_id_obj.pos].timer_id_obj.id);
        return -1;
    }

    expire_time = timer_obj_[timer_id_obj.pos].expire;
    return 0;
}

#endif //TIEMR_MGR_H_
