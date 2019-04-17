const stream=require('stream');

class SimpleBatcher extends stream.Transform{
			_activateWatcher(){
				if(this._timeout){
					this._disableWatcher();
				}
				this._timeout=setTimeout(this._throwBatch.bind(this),this._maxWaitTime);
			}
			_throwBatch(){
				this.push(this._internalBuffer.slice(0,this._limit));
				this._internalBuffer=this._internalBuffer.slice(this._limit);
				this._activateWatcher();
				if(this._internalBuffer.length===0){
					this._disableWatcher();
				  if(this._finalCB){
						this._finalCB();
				}
				}
			}
			_disableWatcher(){
				if(this._timeout){
					clearTimeout(this._timeout)
					this._timeout=null;
				}
			}
			constructor(settings){
				super({objectMode:true});
				this._limit=settings.limit||100;
				this._maxWaitTime=settings.fixedRate || settings.maxWaitTime || 1000;
				this._fixedRate=settings.fixedRate;
				this._internalBuffer=[];
				this._timeout=null;
			}
			_final(cb){
				this._finalCB=cb;
				if((this._internalBuffer.length<=this._limit && !this._fixedRate ) || this._internalBuffer.length===0){
					this._throwBatch();
					this._disableWatcher();
				  this._finalCB();
				}
			}
			_transform(chunck,encoding,cb){
				this._internalBuffer.push(chunck);
				if(this._internalBuffer.length>=this._limit && !this._fixedRate){
					this._throwBatch();
				}
				else{
					if(!this._timeout){
					this._activateWatcher();
				}
				}
				cb();
			}

		}

module.exports=SimpleBatcher;