/*Autogenerated from python*/
%{
#include <bgsched/Switch.h>
%}

%include "/bgsys/drivers/ppcfloor/hlcs/include/bgsched/Switch.h"


%extend bgsched::Switch{

    int getInUseValue(){
        return ($self->getInUse()).toValue();
    }
   
    std::string getInUseString(){
        bgsched::Switch::InUse v = ($self->getInUse()).toValue();
        switch(v){
            PYBGSCHED_CASE_ENUM_TO_STRING(bgsched::Switch, NotInUse)
            PYBGSCHED_CASE_ENUM_TO_STRING(bgsched::Switch, IncludedBothPortsInUse)
            PYBGSCHED_CASE_ENUM_TO_STRING(bgsched::Switch, IncludedOutputPortInUse)
            PYBGSCHED_CASE_ENUM_TO_STRING(bgsched::Switch, IncludedInputPortInUse)
            PYBGSCHED_CASE_ENUM_TO_STRING(bgsched::Switch, Wrapped)
            PYBGSCHED_CASE_ENUM_TO_STRING(bgsched::Switch, Passthrough)
            PYBGSCHED_CASE_ENUM_TO_STRING(bgsched::Switch, WrappedPassthrough)
            default:
                return std::string("UnknownState");
        }       
        return std::string("UnknownState");
    }    
}

%pythoncode{
Switch.getInUse = Switch.getInUseValue
}
