class log(object):                                                              
    def __init__(self, msg=None, *args, color='green'):                         
        if msg is not None and cf.logging.log_level == 'verbose':               
            print(                                                              
                colored(                                                        
                    " ".join(["[LOG]", time.ctime(), '-', msg.format(*args)]),  
                    color=color                                                 
                ), file=sys.stderr                                              
            )                                                                   
                                                                                
    @classmethod                                                                
    def warn(cls, msg, *args):                                                  
        cls(msg, *args, color='red')                                            
                                                                                
    def __call__(self, msg, *args, color='green'):                              
        if cf.logging.log_level == 'verbose':                                   
            print(                                                              
                colored(                                                        
                    " ".join(["[LOG]", time.ctime(), '-', msg.format(*args)]),  
                    color=color                                                 
                ),                                                              
            file=sys.stderr                                                     
        ) 

