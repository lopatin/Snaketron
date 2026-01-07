import React from 'react';
import { useNavigate } from 'react-router-dom';
import Spinner from './Spinner';

interface LoadingScreenProps {
  message: string;
  submessage?: string;
  showCancelButton?: boolean;
  onCancel?: () => void;
}

const LoadingScreen: React.FC<LoadingScreenProps> = ({ 
  message, 
  submessage,
  showCancelButton = false,
  onCancel
}) => {
  const navigate = useNavigate();
  
  const handleCancel = () => {
    if (onCancel) {
      onCancel();
    } else {
      navigate('/');
    }
  };

  return (
    <div className="fixed inset-0 flex items-center justify-center bg-white">
      <div className="text-center">
        <div className="mb-8 flex justify-center">
          <Spinner className="w-12 h-12 text-gray-600" />
        </div>
        <h2 className="text-2xl font-black italic uppercase tracking-1 mb-2 text-black-70">
          {message}
        </h2>
        {submessage && (
          <p className="text-gray-600">
            {submessage}
          </p>
        )}
        {showCancelButton && (
          <button
            onClick={handleCancel}
            className="mt-8 px-6 py-2 text-sm border border-gray-400 rounded font-semibold uppercase bg-white text-gray-600 hover:bg-gray-50 transition-colors"
            style={{ letterSpacing: '0.5px' }}
          >
            Cancel
          </button>
        )}
      </div>
    </div>
  );
};

export default LoadingScreen;